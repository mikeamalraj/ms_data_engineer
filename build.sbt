name := "MS Data Engineer"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"
libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.3.0",
			"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1")
