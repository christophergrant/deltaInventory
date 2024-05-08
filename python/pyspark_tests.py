from pyspark.sql import SparkSession
from run_with_pyspark import *
# to use deltalake (rust or py) or delta-spark to create datasets?

def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [{"path": "tmp/some-table"}]
    spark.createDataFrame(data).withColumn("test", call_rust_udf("path")).show()


if __name__ == '__main__':
    main()