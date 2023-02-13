from threading import Thread
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window, col, expr
from pyspark.sql.streaming import StreamingQuery


def query_result(input_sql: str = None, query_cnt: int = 10):
    used_time = 0
    for x in range(query_cnt):
        spark.sql(input_sql).show(truncate=False, n=100)
        sleep(5)
        used_time += 5
        print(f"""elapsed time: {used_time}s""")


def tumbling_window_example(input_streaming: DataFrame) -> StreamingQuery:
    """
    outputMode complete，查询的 window range 内的 count 会一直增大下去，直到没有输入，最终 count = 3305
    """
    window_count = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .groupBy(window(col("event_time"), "10 seconds"), "User") \
        .count() \
        .writeStream \
        .queryName("count_per_window") \
        .format("memory") \
        .outputMode("complete") \
        .start()
    Thread(target=query_result, args=[f"""
    select * from count_per_window where User = "a"
    and window.start >= "2015-02-23 21:34:40" and window.end <= "2015-02-23 21:34:50"
    """, 100]).start()
    return window_count


def sliding_window_example(input_streaming: DataFrame) -> StreamingQuery:
    """
    更加平稳的对事件进行汇总计算，其它同 tumbling_window_example
    """
    window_count = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .groupBy(window(col("event_time"), "10 seconds", "5 seconds"), "User") \
        .count() \
        .writeStream \
        .queryName("count_per_window") \
        .format("memory") \
        .outputMode("complete") \
        .start()
    Thread(target=query_result, args=[f"""
    select * from count_per_window where User = "a"
    and window.start >= "2015-02-23 21:34:40" and window.end <= "2015-02-23 21:34:50"
    """, 100]).start()
    return window_count


def watermark_example(input_streaming: DataFrame) -> StreamingQuery:
    """
    if you do not specify how late you think you will see data, then Spark will maintain that data in memory forever.
    Specifying a watermark allows it to free those objects from memory, allowing your stream to continue running for a long time.
    Spark should remove inter‐mediate state and, depending on the output mode, do something with the result.

    这里的 delayThreshold = 31 minutes 12 seconds，刚好能够保证，后面进入的数据，会在 watermark 之下，不会算在聚合结果中
    即，batch = 2时，count=127

    注意，这里的 outputMode，如果写成 complete，则中间状态仍会保存，即 查询的 count 仍会一直增加，即使当前 window 已经 closed
    """
    watermark = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .withWatermark("event_time", "31 minutes 12 seconds") \
        .groupBy(window(col("event_time"), "10 seconds", "5 seconds"), "User") \
        .count() \
        .where(f'User = "a" and window.start >= "2015-02-23 21:34:40" and window.end <= "2015-02-23 21:34:50"') \
        .writeStream \
        .queryName("watermark_testing") \
        .format("console") \
        .outputMode("update") \
        .start(truncate=False)
    return watermark


def drop_duplicates_example(input_streaming: DataFrame) -> StreamingQuery:
    """
    Essentially, Structured Streaming makes it easy to take message systems that provide at-least-once semantics,
    and convert them into exactly-once by dropping duplicate messages as they come in, based on arbitrary keys.
    To de-duplicate data, Spark will maintain a number of user specified keys and ensure that duplicates are ignored.

    除了 dropDuplicates 之外，其它和 watermark_example 保持一样
    验证确实 drop 成功的现象：由 batch = 2，count=127 变成了
    batch=0，count=38
    """
    watermark = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .withWatermark("event_time", "31 minutes 12 seconds") \
        .dropDuplicates(["User", "event_time"]) \
        .groupBy(window(col("event_time"), "10 seconds", "5 seconds"), "User") \
        .count() \
        .where(f'User = "a" and window.start >= "2015-02-23 21:34:40" and window.end <= "2015-02-23 21:34:50"') \
        .writeStream \
        .queryName("watermark_testing") \
        .format("console") \
        .outputMode("update") \
        .start(truncate=False)
    return watermark


def join_stream_stream(input_streaming: DataFrame) -> StreamingQuery:
    """
    To allow the state cleanup in this stream-stream join: watermark + event-time constraints needed for outer joins
    1. 限制 state: watermark delays on both inputs such that the engine knows how delayed the input can be (similar to streaming aggregations)
    2. 类似 aggregation window作用，输出有限结果: a constraint on event-time across the two inputs such that the engine can figure out when old rows of one input is not going to be required for matches with the other input.
    """
    a = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .withWatermark("event_time", "10 minutes") \
        .alias("a")
    b = input_streaming \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time") \
        .withWatermark("event_time", "10 minutes") \
        .alias("b")
    query = a.join(b, expr("""
    a.index = b.index
    and a.event_time >= b.event_time
    and a.event_time <= b.event_time + interval 5 minutes
    """), "left") \
        .select('a.index', 'a.event_time', 'a.user', 'b.index', 'b.event_time', 'b.user')

    query = query \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start(truncate=False)

    return query


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Structured Streaming") \
        .config("spark.sql.shuffle.partitions", 5)

    # needs history-server running
    # spark = spark.config("spark.eventLog.enabled", True) \
    #     .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    #     .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")

    spark = spark.getOrCreate()

    path = "../data/activity-data/"
    static = spark.read.json(path)
    dataSchema = static.schema
    print(dataSchema)
    # .format("rate").option("rowsPerSecond", 100)
    streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json(path)

    # query = tumbling_window_example(streaming)
    # query = sliding_window_example(streaming)
    # query = watermark_example(streaming)
    # query = drop_duplicates_example(streaming)
    query = join_stream_stream(streaming)
    query.awaitTermination()
