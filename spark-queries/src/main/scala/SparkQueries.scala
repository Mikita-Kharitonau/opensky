import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, min, round}

import scala.annotation.tailrec

object SparkQueries {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Queries").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///home/kharivitalij/Projects/opensky/opensky-data-2019-08-27/14/*")
    println("\na. Get number of partitions for 1-hour dataset")
    println("val rdd = spark.sparkContext.textFile(\"file:///home/kharivitalij/Projects/opensky/opensky-data-2019-08-27/14/*\")")
    println(s"Num partitions: ${rdd.getNumPartitions}")

    println("\nb. Calculate average latitude and minimum longitude for each origin _country")
    val rowDf = spark.read.format("csv").load("file:///home/kharivitalij/Projects/opensky/opensky-data-2019-08-27/*/*")

    println(s"rowDf.count = ${rowDf.count}")

    val df =rowDf.toDF(
      "time",
      "icao24",
      "callsign",
      "origin_country",
      "time_position",
      "last_contact",
      "longitude",
      "latitude",
      "baro_altitude",
      "on_ground",
      "velocity",
      "true_track",
      "vertical_rate",
      "sensors",
      "geo_altitude",
      "squawk",
      "spi",
      "position_source"
    )

    println("df.groupBy(\"origin_country\").agg(avg(\"latitude\")).show(1000, false)")
    df.groupBy("origin_country").agg(avg("latitude")).show(1000)

    println("df.groupBy(\"origin_country\").agg(min(\"longitude\")).show(1000, false)")
    df.groupBy("origin_country").agg(min("longitude")).show(1000)


    println("\nc. Get the max speed ever seen for the last 4 hours")
    println("df.filter(allAfter(4)).agg(max(\"velocity\")).show")

    df.filter(allAfter(4)).agg(max("velocity")).show


    println("\nd. Get top 10 airplanes with max average speed for the last 4 hours (round the result)")
    println("df.filter(allAfter(4)).groupBy(\"icao24\").agg(avg(\"velocity\")).sort(col(\"avg(velocity)\").desc).limit(10).show")

    df.filter(allAfter(4)).groupBy("icao24").agg(avg("velocity")).sort(col("avg(velocity)").desc).limit(10).show


    println("\ne. Show distinct airplanes where origin_country = 'Germany' and it was on ground at least one time during last 4 hours.")
    println("df.filter(allAfter(4) && col(\"origin_country\") === \"Germany\" && col(\"on_ground\") === \"True\").select(\"icao24\").distinct.show")

    df.filter(allAfter(4) && col("origin_country") === "Germany" && col("on_ground") === "True").select("icao24").distinct.show


    println("f. Show top 10 origin_country with the highest number of unique airplanes in air for the last day")
    println("df.filter(allAfter(24) && col(\"on_ground\") === \"False\").groupBy(\"origin_country\").count.sort(col(\"count\").desc).limit(10).show")

    df.filter(allAfter(24) && col("on_ground") === "False").groupBy("origin_country").count.sort(col("count").desc).limit(10).show


    println("g. Show top 10 longest (by time) completed flights for the last day")
    println("???")

    println("h. Get the average geo_altitude value for each origin_country(round the result to 3 decimal places and rename column)")
    println("df.groupBy(\"origin_country\").agg(round(avg(\"geo_altitude\"), 3)).withColumnRenamed(\"round(avg(geo_altitude), 3)\", \"avg_geo_altitude\").show")

    df.groupBy("origin_country").agg(round(avg("geo_altitude"), 3)).withColumnRenamed("round(avg(geo_altitude), 3)", "avg_geo_altitude").show

    spark.stop()
  }

  def allAfter(hours: Double): Column = {
    col("time") > System.currentTimeMillis / 1000 - hours * 3600
  }

  @tailrec
  final def maxInterval(times: java.util.List[String], index: Int, current: Long): Long = {
    if(index == times.size - 1) {
      return current
    }
    val actual = times.get(index + 1).toLong - times.get(index).toLong
    if(actual > current) {
      maxInterval(times, index + 1, actual)
    }
    else {
      maxInterval(times, index + 1, current)
    }
  }
}