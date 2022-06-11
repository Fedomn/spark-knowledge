from threading import Thread

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.streaming import StreamingQuery


def query_result(input_table: str):
    from time import sleep
    for x in range(10):
        spark.sql(f'select * from {input_table}').show()
        sleep(5)


def activity_counts(input_streaming: DataFrame) -> StreamingQuery:
    count = input_streaming.groupBy("gt").count()
    query = count.writeStream.trigger(processingTime='3 seconds') \
        .format("console").outputMode("complete").start()
    return query


def trans_example(input_streaming: DataFrame) -> StreamingQuery:
    query = input_streaming.withColumn("stairs", expr("gt like '%stairs%'")) \
        .where("stairs") \
        .where("gt is not null") \
        .select("gt", "model", "arrival_time", "creation_time") \
        .writeStream \
        .queryName("simple_transform") \
        .format("memory") \
        .outputMode("append") \
        .start()
    Thread(target=query_result, args=['simple_transform']).start()
    return query


def join_example(input_streaming: DataFrame) -> StreamingQuery:
    historical_agg = static.groupBy("gt", "model").avg()
    device_model_stats = input_streaming.drop("Arrival_Time", "Creation_Time") \
        .cube("gt", "model").avg() \
        .join(historical_agg, ["gt", "model"]) \
        .writeStream.queryName("device_counts").format("memory") \
        .outputMode("complete") \
        .start()
    Thread(target=query_result, args=['device_counts']).start()
    return device_model_stats


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Structured Streaming") \
        .config("spark.sql.shuffle.partitions", 5) \
        .config("spark.eventLog.enabled", True) \
        .config("spark.eventLog.dir", "file:///tmp/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
        .getOrCreate()

    static = spark.read.json("./data/activity-data/")
    dataSchema = static.schema
    print(dataSchema)
    streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json("./data/activity-data")

    activity_query = activity_counts(streaming)
    # trans_example(streaming)
    # join_example(streaming)
    activity_query.awaitTermination()
