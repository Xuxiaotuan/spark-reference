package cn.xuyinyin.magic

import org.apache.spark.sql.{SparkSession, functions}

object SimpleStandaloneApp {

  def main(args: Array[String]): Unit = {
    // 将数字列表转换为RDD
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("spark://xxt.tail47196.ts.net:7077")
      .config("spark.jars", "/Users/xjw/magic/scala-workbench/spark-reference/target/scala-2.13/spark-reference_2.13-1.0.jar")
      .getOrCreate()

    // 导入隐式转换，以便使用DataFrame的API
    import spark.implicits._

    // 创建数字列表
    val numbers = List(1, 2, 3, 4, 5)

    // 将数字列表转换为DataFrame
    val numbersDF = numbers.toDF("number")

    // 计算每个数字的平方
    val squaredDF = numbersDF.select($"number" * $"number" as "squared")

    // 计算平方和
    val sumOfSquares = squaredDF.agg(functions.sum("squared")).first().getLong(0)

    // 打印结果
    println("Sum of squares: " + sumOfSquares)

    // 停止SparkSession对象
    spark.stop()
  }
}
