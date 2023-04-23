from pyspark.sql import SparkSession


def create_table(path: str):
    static = spark.read.json(path)
    static.createTempView("activityTempView")
    print(static.schema)
    sql = f"""
    create table if not exists iceberg.db.activity
    using iceberg
    partitioned by (device)
    TBLPROPERTIES (
        "write.metadata.delete-after-commit.enabled"="true",
        "write.metadata.previous-versions-max"="10",
        "write.target-file-size-bytes"="1048576000",
        "commit.manifest.min-count-to-merge"="10"
    )
    as select * from activityTempView limit 0
    """
    spark.sql(sql)


if __name__ == '__main__':
    basePath = "."
    # basePath = "hdfs://xxx:9000"
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("iceberg_streaming") \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hadoop') \
        .config("spark.sql.catalog.spark_catalog.warehouse", f'{basePath}/hive-warehouse') \
        .config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.iceberg.type', 'hadoop') \
        .config('spark.sql.catalog.iceberg.warehouse', f'{basePath}/iceberg-warehouse') \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')

    # needs history-server running
    # spark = spark.config("spark.eventLog.enabled", True) \
    #     .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    #     .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")

    spark = spark.getOrCreate()

    path = "./data/activity-data/"
    create_table(path)
    spark.sql("show create table iceberg.db.activity").show(truncate=False)

    schema = spark.read.json(path).schema
    streaming = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(path)

    query = streaming.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint/") \
        .option("path", "iceberg.db.activity") \
        .start()
    # .option("fanout-enabled", "true") \
    query.awaitTermination()
