# Quick Introduction to Apache Spark

The purpose of this project is to provide a quick introduction to Apache Spark through simple examples.

The project includes the following small example tasks, implemented as locally-run Spark jobs, both in **Java** and in **Scala**:
* **ShootForPi**: Brute-force estimates the value of pi.
* **SquareCubeMean**: Calculate the mean of the square and cube of random values.
* **RandomMeans**: Calculates the mean of random values.

## Requirements

* Spark 2.4 (tested on 2.4.6)
  * Included as implicit dependencies
* For **spark-test-scala**
  * JDK 1.8+
  * SBT 1.3 (tested on 1.3.12)
  * Scala 2.12 (tested on 2.12.10)
    * Current Spark version (2.4.6 or 3.0.0) does not support Scala 2.13
* For **spark-test-java**
  * JDK (configured against 14, source should work with anything 1.8+)
  * Apache Maven

## Running Instructions

* For **spark-test-scala**
  * Run SBT inside the project directory and execute:
    * `run`
* For **spark-test-java**
  * Build the project: `mvn package`
  * Execute the produced binary: `java -jar target/spark-test-1.0-SNAPSHOT-jar-with-dependencies.jar`
    * The example output will be to `STDOUT`, while the log output will be to `STDERR`
    * While the application is running, the SparkUI will be accessible through http://localhost:4040/
      * At the end of the execution, the application will sleep for a while, to allow some time to explore the SparkUI

## Contributions

All contributions are welcome, although the chances of accepting pull requests are improved if following the basic guidelines:

* All new examples are implemented in both **Java** and **Scala**
* Running the examples don't involve any extra steps
