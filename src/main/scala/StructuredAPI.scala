//Spark SQL is a Spark module for structured data processing
//Spark SQL provide Spark with more information about the structure of both the data and the computation being performed

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object StructuredAPI {
  def main(args: Array[String]): Unit = {
    println("Hello, StructuredAPI !")

    val spark = SparkSession
      .builder()
      .appName("StructuredAPI")
      .master("local[2]")
      .getOrCreate()

    val path = getClass.getResource("./data/flight-data/json/2015-summary.json").toString
    println(path)

    val df = spark.read.format("json").load(path)
    df.printSchema()

    val largeThan1 = udf((n: Int) => {
      n > 1
    })
    spark.udf.register("largeThan1", largeThan1)
    df.select("*").where("largeThan1(count)").orderBy("count").show(3)
  }
}
