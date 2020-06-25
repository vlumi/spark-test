package fi.misaki.sparktest.example

import org.apache.spark.sql.SparkSession

class RandomMeans(val session: SparkSession) extends Serializable {

  def summarizeWithPersist(data: List[Int]): Double = summarize(data, true)

  def summarizeWithoutPersist(data: List[Int]): Double = summarize(data, false)

  def summarize(data: List[Int], persist: Boolean): Double = {
    val sc = session.sparkContext
    val counter = sc.longAccumulator
    val randomData = sc.parallelize(data)
      .map {
        _ => {
          counter.add(1L)
          math.random
        }
      }
    if (persist) randomData.cache

    val mean1 = randomData.reduce(mean)
    val mean2 = randomData.reduce(mean)

    val count = counter.sum
    println(s"Number of input values counted: $count")

    math.abs(mean1 - mean2)
  }

  private def mean(a: Double, b: Double): Double = (a + b) / 2

}
