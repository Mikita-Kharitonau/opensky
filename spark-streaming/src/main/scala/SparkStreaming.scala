import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, col, round, count}


object SparkStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Queries").getOrCreate()

    val staticRowDF = spark.read.format("csv").option("inferSchema", "true").load(
      "file:///home/nikitakharitonov/Projects/opensky/opensky-data-2019-08-28/14/0.csv")

    val schema = staticRowDF.toDF(
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
    ).schema

    val df = spark.readStream.format("csv").schema(schema).load("file:///home/nikitakharitonov/Projects/opensky/opensky-data-2019-09-04/*/*")

    val transformedDF = df
                          .groupBy("origin_country")
                          .agg(round(avg("geo_altitude"), 3), count("*").alias("plains_count"))
                          .withColumnRenamed("round(avg(geo_altitude), 3)", "avg_geo_altitude")
                          .sort(col("avg_geo_altitude").desc)

    val stream = transformedDF.writeStream.outputMode("complete").format("console").start()

    stream.awaitTermination()

    spark.stop()
  }
}
