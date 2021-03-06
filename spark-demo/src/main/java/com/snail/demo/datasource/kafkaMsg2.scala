package com.snail.demo.datasource

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level

object kafkaMsg2 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SparkKafka2").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    //创建topic名称，1表示一次从这个topic中获取一条记录
    val topics = Set("myTopic")
    //指定Kafka的broker地址
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.112.10:9092")

    //创建DStream，接收Kafka的数据
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //处理每次接收到的数据
    val lineDStream = kafkaStream.map(e => {
      new String(e.toString())
    })
    //输出结果
    lineDStream.print()

    ssc.start()
    ssc.awaitTermination();
  }
}