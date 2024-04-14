organization := "org.codecraftlabs.spark"

name := "chicago-crime-dataset-job"

val appVersion = "1.0.0"

val appName = "ChicagoCrimeDatasetProcessor"

version := appVersion

scalaVersion := "2.13.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")