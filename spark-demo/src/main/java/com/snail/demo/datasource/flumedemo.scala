package com.snail.demo.datasource

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object flumedemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFlumeNGWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    //创建FlumeEvent的DStream
    val flumeEvent = FlumeUtils.createPollingStream(ssc, "192.168.112.10", 1234, StorageLevel.MEMORY_ONLY_SER_2)

    //将FlumeEvent中的事件转成字符串
    val lineDStream = flumeEvent.map(e => {
      new String(e.event.getBody.array)
    })
    //输出结果
    lineDStream.print()
    ssc.start()
    ssc.awaitTermination();
  }
}