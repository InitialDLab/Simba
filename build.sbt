name := "simba"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided"

libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
