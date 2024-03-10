organization := "org.codecraftlabs.spark"

name := "SparkExercises"

val appVersion = "1.0.0"

val appName = "SparkExercises"

version := appVersion

scalaVersion := "2.13.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1"
)
