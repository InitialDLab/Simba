// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.11.12"

sparkVersion := "2.1.0"

spName := "InitialDlab/Simba"

// Don't forget to set the version
version := "0.1"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

spIncludeMaven := true

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

name := "simba"

organization := "InitialDLab"

version := "1.0"

scalaVersion := "2.11.8"

sparkComponents ++= Seq("sql", "catalyst", "core")

libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
