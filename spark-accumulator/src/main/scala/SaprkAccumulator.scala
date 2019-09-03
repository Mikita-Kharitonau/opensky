import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import scala.math.random


class DismissalAccumulator extends AccumulatorV2[Double, Double] {
  private val PERIOD: Int = 5 * 365

  private var currentDismissal: Double = 0
  private var iterations: Int = 0
  private var dismissalAccum: Double = 0
  private var currentEmployerSkill: Double = 0
  private var calledNum: BigInt = 0

  def add(newValue: Double): Unit = {
    calledNum += 1

    if(calledNum > PERIOD) {
      currentDismissal = (currentDismissal * iterations + dismissalAccum) / (iterations + 1)
      iterations += 1
      dismissalAccum = 0
      currentEmployerSkill = 0
      calledNum = 0
    }

    if (newValue > currentEmployerSkill) {
      currentEmployerSkill = newValue
      dismissalAccum += 1
    }
  }

  def merge(other: DismissalAccumulator): Unit = {
    println(s"MERGE ${this.value} : ${this.iterations} with ${other.value} : ${other.iterations}")
    this.currentDismissal = (this.currentDismissal * this.iterations + other.currentDismissal * other.iterations) /
                                              (this.iterations + other.iterations)
    this.iterations += other.iterations
  }

  def merge(other: AccumulatorV2[Double, Double]): Unit = {
    this.merge(other.asInstanceOf[DismissalAccumulator])
  }

  def value: Double = currentDismissal - 1

  def copy = new DismissalAccumulator

  def isZero: Boolean = this.currentDismissal < 0.0000001

  def reset: Unit = {
    this.currentDismissal = 0
    this.iterations = 0
    this.dismissalAccum = 0
    this.currentEmployerSkill = 0
    this.calledNum = 0
  }
}

object SparkAccumulator {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Accumulator").getOrCreate()

    val collectionSize: Long = 10000000L
    val numPartitions: Int = 8

    val dismissalAccumulator: AccumulatorV2[Double, Double] = new DismissalAccumulator

    spark.sparkContext.register(dismissalAccumulator, "dismissalAccumulator")

    spark.sparkContext.parallelize(1L to collectionSize, numPartitions).foreach(i => dismissalAccumulator.add(random))

    println(s"Result: ${dismissalAccumulator.value}")

    spark.stop()
  }
}
