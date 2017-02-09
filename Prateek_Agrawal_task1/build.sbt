name := "Prateek_Agrawal_task1"

version := "1.0"

scalaVersion := "2.11.5"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.16",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

    
