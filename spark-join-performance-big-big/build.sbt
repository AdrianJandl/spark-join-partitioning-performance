name := "benchmark-program"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion)