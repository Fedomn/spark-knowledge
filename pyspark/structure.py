if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("StructuredAPI") \
        .getOrCreate()

    df = spark.read.format("json").load("../resources/data/flight-data/json/2015-summary.json")
    df.printSchema()
    df.select("*").where("count > 1").orderBy(desc("count")).show(3)
