package fi.misaki.sparktest.example

import org.apache.spark.sql.SparkSession

class SquareCubeMean(val session: SparkSession) extends Serializable {
  def process(data: List[Int]): Double = {
    val sc = session.sparkContext
    val randomData = sc.parallelize(data)
      .map(_ => math.random)
    randomData.cache

    val squares = randomData
      .map(math.pow(_, 2))
    val cubes = randomData
      .map(math.pow(_, 3))

    squares.union(cubes)
      .reduce((a, b) => (a + b) / 2)
  }
}
