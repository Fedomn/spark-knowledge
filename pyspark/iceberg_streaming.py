from pyspark.sql import SparkSession


def create_table(path: str):
    static = spark.read.json(path)
    static.createTempView("activityTempView")
    print(static.schema)
    sql = f"""
    create table iceberg.db.activity
    using iceberg
    partitioned by (device)
    as select * from activityTempView limit 0
    """
    spark.sql(sql)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("iceberg_streaming") \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hadoop') \
        .config("spark.sql.catalog.spark_catalog.warehouse", './hive-warehouse') \
        .config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.iceberg.type', 'hadoop') \
        .config('spark.sql.catalog.iceberg.warehouse', './iceberg-warehouse') \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .getOrCreate()

    path = "./data/activity-data/"
    create_table(path)
    spark.sql("show create table iceberg.db.activity").show(truncate=False)

    schema = spark.read.json(path).schema
    streaming = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(path)
    # TODO: optimize too many small files
    query = streaming.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("maxFilesPerTrigger", 1) \
        .option("path", "iceberg.db.activity") \
        .option("checkpointLocation", "./checkpoint/") \
        .start()
    query.awaitTermination()
