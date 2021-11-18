import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

object DistributedSharedVariables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DistributedSharedVariables")
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

object Accumulator {
  case class Flight(ORIGIN_COUNTRY_NAME: String, DEST_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Accumulator")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val path = Accumulator.getClass.getResource("./data/flight-data/parquet/2010-summary.parquet").toString
    val flights = spark.read.parquet(path).as[Flight]

    normalAcc(spark, flights)
    customAcc(spark, flights)
  }

  private def normalAcc(spark: SparkSession, flights: Dataset[Flight]): Unit = {
    val accChina = spark.sparkContext.longAccumulator("China")

    def accChinaFunc(flight_row: Flight): Unit = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }

    flights.foreach(flight_row => accChinaFunc(flight_row))

    println(accChina.value)
  }

  private def customAcc(spark: SparkSession, flights: Dataset[Flight]): Unit = {
    class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
      private var num: BigInt = 0

      override def isZero: Boolean = this.num == 0

      override def copy(): AccumulatorV2[BigInt, BigInt] = new EvenAccumulator

      override def reset(): Unit = this.num = 0

      override def add(v: BigInt): Unit = {
        if (v % 2 == 0) {
          this.num += v
        }
      }

      override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {
        this.num += other.value
      }

      override def value: BigInt = this.num
    }

    val evenAcc = new EvenAccumulator
    spark.sparkContext.register(evenAcc, "EvenAcc")

    println(evenAcc.value)
    flights.foreach(flight_row => evenAcc.add(flight_row.count))
    println(evenAcc.value)
  }
}
