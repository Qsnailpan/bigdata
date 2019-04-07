package com.demo.sougou.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

object SougouLog {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("sougoulog11").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.112.10:9000/input/SogouQ.txt")
    val rdd2 = rdd1.map(_.split("\t")).filter(_.length == 6)
    rdd2.count()
    val rdd3 = rdd2.filter(_(3).toInt == 1).filter(_(4).toInt == 2)
    rdd3.count()
    val rdd4 = rdd3.map(_.toList.mkString(","))

    rdd4.saveAsTextFile("E:\\BigData\\data\\out\\SogouQ2")
    sc.stop()
  }
}