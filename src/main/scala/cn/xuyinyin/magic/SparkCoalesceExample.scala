package cn.xuyinyin.magic
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * 环比
 *
 * @author : XuJiaWei
 * @since : 2023-08-30 11:14
 */
object SparkCoalesceExample {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("Simple Spark Coalesce Application").master("local").getOrCreate()

    // 创建数据
    val df: DataFrame = spark
      .createDataFrame(
        Seq(
          ("2023-07-20", 100),
          ("2023-07-21", 120),
          ("2023-07-22", 140),
          ("2023-07-23", 160),
          ("2023-07-24", 180)
        ))
      .toDF("date", "value")

    // 将字符串日期转换为日期类型
    val dfWithDate: DataFrame = df.withColumn("date", to_date(col("date")))

    /**
     * +----------+-----+
     * |      date|value|
     * +----------+-----+
     * |2023-07-20|  100|
     * |2023-07-21|  120|
     * |2023-07-22|  140|
     * |2023-07-23|  160|
     * |2023-07-24|  180|
     * +----------+-----+
     */
    dfWithDate.show();

    // 计算环比增长率
    val  windowSpec = Window.orderBy("date")
    val dfWithGrowthRate = dfWithDate.withColumn(
      "growth_rate",
      (col("value") - lag("value", 1).over(windowSpec)) / lag("value", 1).over(windowSpec)
    )

    /**
     * 打印结果
     *
     * +----------+-----+-------------------+
     * |      date|value|        growth_rate|
     * +----------+-----+-------------------+
     * |2023-07-20|  100|               null|
     * |2023-07-21|  120|                0.2|
     * |2023-07-22|  140|0.16666666666666666|
     * |2023-07-23|  160|0.14285714285714285|
     * |2023-07-24|  180|              0.125|
     * +----------+-----+-------------------+
     */
    dfWithGrowthRate.show()
  }
}
