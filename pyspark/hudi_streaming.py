import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("hudi_streaming") \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')

    # needs history-server running
    spark = spark.config("spark.eventLog.enabled", True) \
        .config("spark.eventLog.dir", "file:///tmp/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")

    spark = spark.getOrCreate()

    path = "./data/activity-data"

    schema = spark.read.json(path).schema
    print(schema)
    streaming = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(path)

    hudi_streaming_options = {
        'hoodie.table.name': 'activity_hudi',
        'hoodie.datasource.write.recordkey.field': 'Arrival_Time',
        'hoodie.datasource.write.partitionpath.field': 'Device',
        'hoodie.datasource.write.table.name': 'activity_hudi',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'Creation_Time',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
    }

    abspath = os.path.abspath('')

    # .option("hoodie.write.buffer.limit.bytes", "10485760") \
    query = streaming.writeStream \
        .format("org.apache.hudi") \
        .options(**hudi_streaming_options) \
        .outputMode("append") \
        .option("path", f"{abspath}/hudi-warehouse/activity_hudi") \
        .option("checkpointLocation", f"{abspath}/hudi-warehouse/checkpoint") \
        .trigger(once=True) \
        .start()
    query.awaitTermination()
