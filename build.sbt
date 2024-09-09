name := "Spark-Reference"

version := "1.0"

scalaVersion := "2.12.15"
val sparkVersion              = "3.3.3"
val starrocksConnectorVersion = "1.1.0"
val mysqlVersion              = "8.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"                          % sparkVersion,
  "org.apache.spark" %% "spark-core"                         % sparkVersion,
  "com.starrocks"     % "starrocks-spark-connector-3.3_2.12" % starrocksConnectorVersion,
  "com.mysql"         % "mysql-connector-j"                  % mysqlVersion
)
