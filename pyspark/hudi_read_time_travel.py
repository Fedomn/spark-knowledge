import os

from pyspark.sql import SparkSession


def create_external_table_not_support_time_travel(basePath: str):
    """
    https://hudi.apache.org/docs/table_management#create-table-for-an-external-hudi-table
    """
    # external table -> not support time travel
    spark.sql(f"create table if not exists hudi_trips_cow using hudi location '{basePath}';")
    spark.sql("show tables").show()
    spark.sql("select * from hudi_trips_cow").show()
    spark.sql("select * from hudi_trips_cow timestamp as of '2023-04-21 16:57:34.915';").show()

    # temp view also not support time travel
    # abspath = os.path.abspath('')
    # spark.read.format('hudi').load(f"{abspath}/hudi-warehouse/hudi_trips_cow").createOrReplaceTempView("hudi_trips_cow")


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("hudi_streaming") \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')

    # https://github.com/apache/hudi/pull/8082
    spark.config("spark.sql.legacy.parquet.nanosAsLong", "false") \
        .config("spark.sql.parquet.binaryAsString", "false") \
        .config("spark.sql.parquet.int96AsTimestamp", "true") \
        .config("spark.sql.caseSensitive", "false")

    spark = spark.getOrCreate()

    abspath = os.path.abspath('')
    basePath = f"{abspath}/hudi-warehouse/hudi_trips_cow"

    df = spark.read.format('hudi') \
        .option("as.of.instant", "2023-04-21 16:57:34.915") \
        .load(f"{abspath}/hudi-warehouse/hudi_trips_cow")
    df.filter("uuid = 'bbfc3870-122a-45d2-a83a-18ceb4d5f8be'").show(truncate=False)
