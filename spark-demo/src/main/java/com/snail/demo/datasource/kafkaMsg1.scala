package com.snail.demo.datasource

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.kafka.KafkaUtils

object kafkaMsg1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkKafka1").setMaster("local[2]")
    //两个参数：1、conf参数    2、采样时间间隔:每隔3秒
    val ssc = new StreamingContext(conf, Seconds(3))

    val myTopic = Map("myTopic" -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, "192.168.112.10:2181", "myGroup", myTopic, StorageLevel.MEMORY_ONLY)

    val line = kafkaStream.map(e => {
      new String(e.toString())
    })

    line.print()

    ssc.start()
    ssc.awaitTermination()
  }
}