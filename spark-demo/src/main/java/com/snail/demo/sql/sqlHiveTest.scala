package com.snail.demo.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object sqlHiveTest {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("sqlHiveTest")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.table("movie_table").registerTempTable("movie_table")

    hiveContext.sql("SELECT movieId, title FROM movie_table").show()
  }
}
