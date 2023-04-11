if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("OptimizeMetadataOnlyQuery") \
        .config("spark.sql.warehouse.dir", './warehouse') \
        .enableHiveSupport() \
        .config("spark.eventLog.enabled", True) \
        .config("spark.eventLog.dir", "file:///tmp/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
        .getOrCreate()

    # spark.sparkContext.setLogLevel('debug')

    # ------ prepare testing table ------
    # spark.read.format("json").load("./data/flight-data/json").createOrReplaceTempView("summary")
    # spark.sql("SET hive.exec.dynamic.partition.mode=non-strict;")
    # sql = f"""
    # create table t using hive
    # options(FILEFORMAT "parquet")
    # partitioned by (dest_country_name)
    # TBLPROPERTIES ("transactional" = "false")
    # as select * from summary
    # """
    # spark.sql(sql)

    # 查看执行计划，该max语句仍然会scan所有的files(800个文件)，see：OptimizeMetadataOnlyQuery
    df = spark.sql("select * from t where dest_country_name in (select max(dest_country_name) from t)")
    df.write.mode('overwrite').format('parquet').saveAsTable("one_partition")
