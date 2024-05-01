from pyspark.sql.functions import expr, col, when, element_at, split, pandas_udf
from pyspark.sql.types import StringType
import subprocess
import pandas as pd

def extract_bucket_name(column_name):
    return when(col(column_name).startswith("s3://"), element_at(split(expr(f"substring({column_name}, 6)"), "/"), 1)) \
        .when(col(column_name).contains("amazonaws.com"), 
                when(col(column_name).contains("s3.amazonaws.com"),
                    element_at(split(expr(f"substring({column_name}, instr({column_name}, 's3.amazonaws.com/') + 18)"), "/"), 1))
                .otherwise(element_at(split(expr(f"substring({column_name}, instr({column_name}, '://') + 3)"), "."), 1))
                ) \
        .otherwise(element_at(split(expr(f"substring({column_name}, 6)"), "/"), 1))

df = spark.read.json(spark_path).withColumn("bucket_name", extract_bucket_name("location"))

@pandas_udf(StringType())
def call_rust_udf(table_path_series: pd.Series) -> pd.Series:
    results = []
    executable_path = '/root/.cargo/bin/delta_inventory'
    for loc in table_path_series:
        command = [executable_path, loc]
        result = subprocess.run(command, capture_output=True, text=True)
        if not result.stdout:
            results.append("{'error_count': 1}")
        else:
            results.append(result.stdout)
    return pd.Series(results)

df.repartition(16).withColumn("tombstone_json", call_rust_udf("location")).write.save("dbfs:/deltaInventory")
