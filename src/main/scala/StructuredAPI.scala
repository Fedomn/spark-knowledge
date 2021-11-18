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

object DistributedSharedVariables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StructuredAPI")
      .master("local[*]")
      .getOrCreate()

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300, "Simple" -> 100)

    val broadCastValue = spark.sparkContext.broadcast(supplementalData)

    val res = words.map(word => (word, broadCastValue.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()

    println(res.mkString("Array(", ", ", ")"))
  }
}