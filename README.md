# spark-test
Quick introduction to Apache Spark through simple examples.

The project includes the following small example tasks, implemented as locally-run Spark jobs:
* ShootForPi: Brute-force estimates the value of pi.
* SquareCubeMean: Calculate the mean of the square and cube of random values.
* RandomMeans: Calculates the mean of random values.

Requirements:
* JDK (configured against 14, source should work with anything 1.8+)
* Apache Maven

Instructions:
* Build the project: `mvn package`
* Execute the produced binary: `java -jar target/spark-test-1.0-SNAPSHOT-jar-with-dependencies.jar`
  * The example output will be to `STDOUT`, while the log output will be to `STDERR`
  * While the application is running, the SparkUI will be accessible through http://localhost:4040/
    * At the end of the execution, the application will sleep for a while, to allow some time to explore the SparkUI
