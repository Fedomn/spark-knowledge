from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# reference:
# https://medium.com/expedia-group-tech/apache-spark-structured-streaming-watermarking-6-of-6-1187542d779f

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Structured Streaming") \
        .config("spark.sql.shuffle.partitions", 5)

    # Watermark = max event time seen so far — delayThreshold
    spark = spark.getOrCreate()
    # nc -lk 9999
    # 2021-01-01 10:06:00#2
    # 2021-01-01 10:13:00#5
    # 2020-12-31 10:06:00#1
    # 2021-01-01 10:08:00#8
    # 2021-01-01 10:00:01#7
    # 2021-01-01 9:59:00#3
    # 2021-01-01 10:25:10#4
    # 2021-01-01 10:14:00#10
    # 2021-01-01 10:39:00#15
    # 2021-01-01 10:26:00#1
    # 2021-01-01 10:24:00#6
    # 2021-01-01 10:30:00#11
    streaming = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

    streaming = streaming.select(split(col("value"), "#").alias("data")) \
        .withColumn("event_timestamp", element_at(col("data"), 1).cast("timestamp")) \
        .withColumn("val", element_at(col("data"), 2).cast("int")) \
        .drop("data")

    streaming = streaming \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(window(col("event_timestamp"), "5 minutes")) \
        .agg(sum("val").alias("sum"))

    query = streaming \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .start(truncate=False)

    query.awaitTermination()
