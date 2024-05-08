from pyspark.sql.functions import expr, col, when, element_at, split, pandas_udf
from pyspark.sql.types import *
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

udf_schema = StructType([
    StructField("path_to_bin", StringType(), False),
    StructField("path_to_table", StringType(), False),
])

# TODO switch this invocation to mapped groups
@pandas_udf(udf_schema)
def call_rust_udf(path_to_bin:  pd.Series, table_path_series: pd.Series) -> pd.Series:
    results = []
    for loc in table_path_series:
        command = [path_to_bin, loc]
        result = subprocess.run(command, capture_output=True, text=True)
        if not result.stdout:
            results.append("{'error_count': 1}")
        else:
            results.append(result.stdout)
    return pd.Series(results)
