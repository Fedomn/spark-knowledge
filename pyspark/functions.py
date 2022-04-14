from pyspark.sql import SparkSession

# https://spark.apache.org/docs/latest/api/sql/#explode
# https://sparkbyexamples.com/pyspark/pyspark-explode-nested-array-into-rows

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()

    data = [
        ["A", 20, "python", "java", "rust"],
        ["B", 10, "rust", "golang", ""],
        ["C", 30, "c++", "c++", "c++"],
    ]
    columns = ["name", "age", "t1", "t2", "t3"]
    df = spark.createDataFrame(data, columns)
    df.printSchema()
    df.createOrReplaceTempView("techs")

    spark.sql(f"""
    select distinct t.name, t.age, explode(array(t.t1, t.t2, t.t3)) as ts
    from techs t
    """).show()
