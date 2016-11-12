name := "simba"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.3"

libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
