package cn.xuyinyin.magic

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val logFile = "/Users/xjw/software/spark/spark-3.4.1-bin-hadoop3-scala2.13/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}