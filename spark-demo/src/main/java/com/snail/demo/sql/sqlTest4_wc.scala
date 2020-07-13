package com.snail.demo.sql

import breeze.linalg.*
import org.apache.spark.sql.{Dataset, SparkSession}

object sqlTest4_wc {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SQLTest")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val lines: Dataset[String] = session.read.textFile("hdfs://master:9000/The_Man_of_Property.txt")

    //    lines.show()

    val words: Dataset[String] = lines.flatMap(_.split(" "))

//    val count = words.groupBy($"value" as "word").count()
    val count = words.groupBy($"value" as "word").count().sort($"count" desc)
    count.show()
    session.stop()

  }
}
