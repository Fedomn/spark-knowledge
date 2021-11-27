import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Tuning {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.dir", "file:///tmp/spark-events")
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
      .getOrCreate()

    val df = spark
      .range(10000000).toDF("id")
      .withColumn("square", col("id") * col("id"))

    //df.cache()
    df.persist()
    spark.time(df.count())
    spark.time(df.count())
  }
}
