import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, min, round, sort_array, collect_list}

import scala.annotation.tailrec

object SparkQueries {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Queries").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///home/nikitakharitonov/Projects/opensky/opensky-data-2019-08-28/14/*")
    println("\na. Get number of partitions for 1-hour dataset")
    println("val rdd = spark.sparkContext.textFile(\"file:///home/nikitakharitonov/Projects/opensky/opensky-data-2019-08-28/14/*\")")
    println(s"Num partitions: ${rdd.getNumPartitions}")

    println("\nb. Calculate average latitude and minimum longitude for each origin _country")
    val rowDf = spark.read.format("csv").load("file:///home/nikitakharitonov/Projects/opensky/opensky-data-2019-08-28/*/*")

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

    df.createOrReplaceTempView("my_df_table")

    println("df.groupBy(\"origin_country\").agg(avg(\"latitude\")).show(1000, false)")
    df.groupBy("origin_country").agg(avg("latitude")).show
    println("spark.sql(\"select origin_country, avg(latitude) from my_df_table group by origin_country\").show")
    spark.sql("select origin_country, avg(latitude) from my_df_table group by origin_country").show

    println("df.groupBy(\"origin_country\").agg(min(\"longitude\")).show(1000, false)")
    df.groupBy("origin_country").agg(min("longitude")).show
    println("spark.sql(\"select origin_country, min(longitude) from my_df_table group by origin_country\").show")
    spark.sql("select origin_country, min(longitude) from my_df_table group by origin_country").show

    println("\nc. Get the max speed ever seen for the last 4 hours")

    println("df.filter(allAfterPredicate(4)).agg(max(\"velocity\")).show")
    df.filter(allAfterPredicate(4)).agg(max("velocity")).show

    println("spark.sql(s\"select max(velocity) from my_df_table where time > ${allAfter(4)}\")")
    spark.sql(s"select max(velocity) from my_df_table where time > ${allAfter(4)}").show


    println("\nd. Get top 10 airplanes with max average speed for the last 4 hours (round the result)")

    println("df.filter(allAfterPredicate(4)).groupBy(\"icao24\").agg(avg(\"velocity\")).sort(col(\"avg(velocity)\").desc).limit(10).show")
    df.filter(allAfterPredicate(4)).groupBy("icao24").agg(avg("velocity")).sort(col("avg(velocity)").desc).limit(10).show

    println("spark.sql(s\"select icao24, avg(velocity) as velocity from my_df_table where time > ${allAfter(4)} group by icao24\").sort(col(\"velocity\").desc).limit(10).show")
    spark.sql(s"select icao24, avg(velocity) as velocity from my_df_table where time > ${allAfter(4)} group by icao24").sort(col("velocity").desc).limit(10).show

    println("spark.sql(s\"select icao24, avg(velocity) as velocity from my_df_table where time > ${allAfter(4)} group by icao24 sort by velocity desc limit 10\").show")
    spark.sql(s"select icao24, avg(velocity) as velocity from my_df_table where time > ${allAfter(4)} group by icao24 sort by velocity desc limit 10").show


    println("\ne. Show distinct airplanes where origin_country = 'Germany' and it was on ground at least one time during last 4 hours.")

    println("df.filter(allAfterPredicate(4) && col(\"origin_country\") === \"Germany\" && col(\"on_ground\") === \"True\").select(\"icao24\").distinct.show")
    df.filter(allAfterPredicate(4) && col("origin_country") === "Germany" && col("on_ground") === "True").select("icao24").distinct.show

    println("spark.sql(s\"select distinct icao24 from my_df_table where time > ${allAfter(4)} and origin_country = 'Germany' and on_ground = 'True'\").show")
    spark.sql(s"select distinct icao24 from my_df_table where time > ${allAfter(4)} and origin_country = 'Germany' and on_ground = 'True'").show


    println("\nf. Show top 10 origin_country with the highest number of unique airplanes in air for the last day")

    println("df.filter(allAfterPredicate(24) && col(\"on_ground\") === \"False\").groupBy(\"origin_country\").count.sort(col(\"count\").desc).limit(10).show")
    df.filter(allAfterPredicate(24) && col("on_ground") === "False").groupBy("icao24", "origin_country").count.groupBy("origin_country").count.sort(col("count").desc).limit(10).show

    println("spark.sql(s\"select origin_country, count(*) as cnt from (select icao24, origin_country from my_df_table where time > ${allAfter(240)} and on_ground = 'False' group by icao24, origin_country) group by origin_country sort by cnt desc limit 10\").show -- It is seems not working, don't understand why..")
    spark.sql(s"select origin_country, count(*) as cnt from (select icao24, origin_country from my_df_table where time > ${allAfter(24)} and on_ground = 'False' group by icao24, origin_country) group by origin_country sort by cnt desc limit 10").show
    println("But it is works correct: \nspark.sql(s\"select origin_country, count(*) as cnt from (select icao24, origin_country from my_df_table where time > ${allAfter(240)} and on_ground = 'False' group by icao24, origin_country) group by origin_country\").sort(col(\"cnt\").desc).limit(10).show")
    spark.sql(s"select origin_country, count(*) as cnt from (select icao24, origin_country from my_df_table where time > ${allAfter(24)} and on_ground = 'False' group by icao24, origin_country) group by origin_country").sort(col("cnt").desc).limit(10).show


    println("\ng. Show top 10 longest (by time) completed flights for the last day")
    println("df.filter(col(\"on_ground\") === \"True\").groupBy(\"icao24\").agg(sort_array(collect_list(col(\"time\")))).map(row => (row.getString(0), maxInterval(row.getList(1), 0, 0))).sort(col(\"_2\").desc).limit(10).show")
    import spark.implicits._
    df.filter(col("on_ground") === "True").groupBy("icao24").agg(sort_array(collect_list(col("time")))).map(row => (row.getString(0), maxInterval(row.getList(1), 0, 0))).sort(col("_2").desc).limit(10).show

    println("\nh. Get the average geo_altitude value for each origin_country(round the result to 3 decimal places and rename column)")

    println("df.groupBy(\"origin_country\").agg(round(avg(\"geo_altitude\"), 3)).withColumnRenamed(\"round(avg(geo_altitude), 3)\", \"avg_geo_altitude\").show")
    df.groupBy("origin_country").agg(round(avg("geo_altitude"), 3)).withColumnRenamed("round(avg(geo_altitude), 3)", "avg_geo_altitude").show

    println("spark.sql(s\"select origin_country, round(avg(geo_altitude), 3) as avg_geo_altitude from my_df_table group by origin_country\").show")
    spark.sql(s"select origin_country, round(avg(geo_altitude), 3) as avg_geo_altitude from my_df_table group by origin_country").show

    spark.stop()
  }

  def allAfterPredicate(hours: Double): Column = {
    col("time") > System.currentTimeMillis / 1000 - hours * 3600
  }

  def allAfter(hours: Double): Long = {
    (System.currentTimeMillis / 1000 - hours * 3600).toLong
  }

  @tailrec
  final def maxInterval(times: java.util.List[String], index: Int, current: Long): Long = {
    if(index == times.size - 1) return current
    val actual = times.get(index + 1).toLong - times.get(index).toLong
    if (actual > current) maxInterval(times, index + 1, actual)
    else maxInterval(times, index + 1, current)
  }
}