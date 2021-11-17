scalaVersion := "2.13.3"

name := "spark-playground"
organization := "ch.epfl.scala"
version := "1.0"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
