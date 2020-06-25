package fi.misaki.sparktest

import fi.misaki.sparktest.example.{RandomMeans, ShootForPi, SquareCubeMean}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {
  private val WAIT_S = 60
  private val WAIT_GRANULARITY_S = 10
  private val MULTIPLIER_NS = 1000000000L // TODO: thousand separators from Scala 2.13
  private val MULTIPLIER_MS = 1000 // TODO: thousand separators from Scala 2.13

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  private val session = init
  runShootForPi
  runRandomMeans
  runSquareCubeMean
  // Wait a while, to give some time for browsing the job data
  waitAtEnd(WAIT_S)

  private def init: SparkSession =
    SparkSession.builder
      .appName("spark-test")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  private def runShootForPi: Unit = {
    val dataSize = 10000000 // TODO: thousand separators from Scala 2.13
    val pi = new ShootForPi(session)
      .estimatePi(generateEmptyData(dataSize))
    println(s"Pi is roughly $pi");
  }

  private def runRandomMeans: Unit = {
    val dataSize = 10000000 // TODO: thousand separators from Scala 2.13
    val meanDifference1 = new RandomMeans(session)
      .summarizeWithoutPersist(generateEmptyData(dataSize))
    println(s"Processed twice without persisting, means differ by $meanDifference1")
    val meanDifference2 = new RandomMeans(session)
      .summarizeWithPersist(generateEmptyData(dataSize))
    println(s"Processed twice with persisting, means differ by $meanDifference2")
  }

  private def runSquareCubeMean: Unit = {
    val dataSize = 10000000 // TODO: thousand separators from Scala 2.13
    val mean = new SquareCubeMean(session)
      .process(generateEmptyData(dataSize))
    println(s"Mean from random values squared and cubed: $mean")
  }

  private def generateEmptyData(size: Int): List[Int] =
    List.fill(size)(0)

  private def waitAtEnd(seconds: Long): Unit = {
    val endNanos = System.nanoTime + seconds * MULTIPLIER_NS
    try while (System.nanoTime < endNanos) {
      val secondsLeft = (endNanos - System.nanoTime) / MULTIPLIER_NS
      println(s"Time until termination: ${secondsLeft}s")
      Thread.sleep(WAIT_GRANULARITY_S * MULTIPLIER_MS)
    }
    catch {
      case _: InterruptedException =>
    }
  }
}
