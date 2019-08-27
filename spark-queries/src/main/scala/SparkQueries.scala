import org.apache.spark.sql.SparkSession

object SparkQueries {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Queries").getOrCreate()
    val logData = spark.range(1000).count
    println(s"Log data: $logData")
    spark.stop()
  }
}