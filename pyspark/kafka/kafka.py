from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    # download
    # spark-sql-kafka-0-10_2.12-3.2.1.jar
    # kafka-clients-3.2.0.jar
    # spark-token-provider-kafka-0-10_2.12-3.2.1.jar
    # commons-pool2-2.11.1.jar
    # and then put it into ~/.pyenv/versions/3.8.10/lib/python3.8/site-packages/pyspark/jars

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Structured Streaming") \
        .config("spark.sql.shuffle.partitions", 5)

    # Watermark = max event time seen so far — delayThreshold
    # send using producer
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

    # consumer 控制每次从开始读 startingOffsets=earliest
    spark = spark.getOrCreate()
    streaming = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "foo") \
        .option("startingOffsets", "earliest") \
        .load()

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
