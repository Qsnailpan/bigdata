package com.snail.demo.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object sqlTest3_wc {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SQLTest")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val lines: Dataset[String] = session.read.textFile("hdfs://master:9000/The_Man_of_Property.txt")

//    lines.show()

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    words.createTempView("wc_table")

    val result = session.sql("select value, count(*) counts from wc_table group by value")

    result.show()

    session.stop()

  }
}
