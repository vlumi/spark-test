package fi.misaki.sparktest.example

import org.apache.spark.sql.SparkSession

class ShootForPi(val session: SparkSession) extends Serializable {

  def estimatePi(data: List[Int]): Double = {
    val sc = session.sparkContext
    val count = sc.parallelize(data)
      .filter(_ => {
        val x = Math.random
        val y = Math.random
        x * x + y * y < 1
      })
      .count
    4.0 * count / data.size
  }

}
