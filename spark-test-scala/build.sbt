name := "spark-test-scala"

version := "0.1"

scalaVersion := "2.12.10"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.4.6"),
  ("org.apache.spark" %% "spark-sql" % "2.4.6")
)
