if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("StructuredAPI") \
        .config("spark.sql.warehouse.dir", './warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.format("json").load("./data/flight-data/json/2015-summary.json")
    df.printSchema()
    df.select("*").where("count > 1").orderBy(desc("count")).show(3)

    new_df: DataFrame = df.select("*").where("count > 1")
    new_df.write.mode('overwrite').format('orc').saveAsTable("filtered_data")
    spark.sql("select * from filtered_data").printSchema()
