ThisBuild / scalaVersion := "2.12.13"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "clark",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"
  )
