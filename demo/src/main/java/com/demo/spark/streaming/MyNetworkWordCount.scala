package com.demo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyNetworkWordCount {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建一个Context对象: StreamingContext  (SparkContext, SQLContext)
    //指定批处理的时间间隔
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //设置检查点，可以是hdfs目录
    ssc.checkpoint("E:\\BigData\\data\\ckpt")

    //创建一个DStream，处理数据
    val lines = ssc.socketTextStream("192.168.112.10", 1234, StorageLevel.MEMORY_ONLY)

    //执行wordcount
    val words = lines.flatMap(_.split(" "))

    //定义函数用于累计每个单词的总频率
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型

      println("previousCount" + previousCount)
      Some(currentCount + previousCount)
    }

    val pairs = words.map(word => (word, 1))

    val totalWordCounts = pairs.updateStateByKey[Int](addFunc)
    totalWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}