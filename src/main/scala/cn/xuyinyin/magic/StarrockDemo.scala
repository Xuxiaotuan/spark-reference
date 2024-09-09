package cn.xuyinyin.magic

import org.apache.spark.sql.SparkSession


/**
 * @author : XuJiaWei
 * @since : 2024-09-09 09:57
 */
object StarrockDemo {

  def main(args: Array[String]): Unit = {
    val seesion = newSession("test-1")

    val starrocksSparkDF = seesion.read.format("starrocks")
      .option("starrocks.table.identifier", "test.score_board")
      .option("starrocks.fe.http.url", "100.82.226.63:31059")
      .option("starrocks.fe.jdbc.url", "jdbc:mysql://100.82.226.63:31546")
      .option("starrocks.user", "root")
      .option("starrocks.password", "")
      .option("starrocks.filter.query", "id != 1")
      .load()

    seesion.stop()
  }

  /**
   * @todo 创建spark session
   * @return
   */
  def newSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[8]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}
