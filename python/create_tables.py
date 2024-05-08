from pyspark.sql import SparkSession
from delta import *



def main():
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.range(1).write.format("delta").save("tables/golden-goose")


if __name__ == '__main__':
    main()
