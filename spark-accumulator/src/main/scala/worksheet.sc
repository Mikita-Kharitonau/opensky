import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }

import scala.math.random
import org.apache.spark.util.AccumulatorV2

// Set off the vebose log messages
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

val spark = SparkSession.builder
  .master("local")
  .appName("Spark Quide Worksheet")
  .config("spark.ui.showConsoleProgress", false)
  .getOrCreate()

import spark.implicits._

lazy val sc = spark.sparkContext

// Your Spark codes are here
// spark: sparkSession object (like sqlContext in Spark 1.x)
// sc: sparkContext object (Spark 1.x compatible)

class DismissalAccumulator extends AccumulatorV2[Double, Double] {
  private val PERIOD: Int = 365 * 5

  private var currentDismissal: Double = 0
  private var iterations: Int = 0
  private var dismissalAccum: Double = 0
  private var currentEmployerSkill: Double = 0
  private var calledNum: BigInt = 0

  def add(newValue: Double): Unit = {
    calledNum = calledNum + 1

    if(calledNum > PERIOD) {
      currentDismissal = (currentDismissal * iterations + dismissalAccum) / (iterations + 1)
      iterations = iterations + 1
      dismissalAccum = 0
      currentEmployerSkill = 0
    }

    if (newValue > currentEmployerSkill) {
      currentEmployerSkill = newValue
      dismissalAccum = dismissalAccum + 1
    }
  }

  def merge(other: DismissalAccumulator): Unit = {
    this.currentDismissal = (this.currentDismissal * this.iterations + other.currentDismissal * other.iterations) / (this.iterations + other.iterations)
  }

  def merge(other: AccumulatorV2[Double, Double]): Unit = {
    this.merge(other.asInstanceOf[DismissalAccumulator])
  }

  def value: Double = currentDismissal

  def copy = new DismissalAccumulator

  def isZero: Boolean = this.currentDismissal < 0.0000001

  def reset: Unit = this.dismissalAccum = 0
}

val collectionSize: Long = 1000000L
val numPartitions: Int = 8

sc.parallelize(1 to collectionSize, numPartitions).foreach(i => myCustomAccum.add(random))
