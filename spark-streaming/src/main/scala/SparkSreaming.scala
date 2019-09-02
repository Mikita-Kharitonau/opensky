import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, min, round, sort_array, collect_list}


object SparkStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Queries").getOrCreate()

    spark.stop()
  }
}
