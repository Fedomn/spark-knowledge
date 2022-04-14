from pyspark.sql import SparkSession
from pyspark.sql.types import *


def is_large_10(num: int):
    return num > 10


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("udf") \
        .getOrCreate()

    data = [
        ["A", 20],
        ["B", 10],
        ["C", 30],
    ]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    df.printSchema()
    df.createOrReplaceTempView("techs")
    spark.udf.register("is_large_10", is_large_10, BooleanType())

    spark.sql("select t.name, t.age, is_large_10(t.age) from techs t").show()
