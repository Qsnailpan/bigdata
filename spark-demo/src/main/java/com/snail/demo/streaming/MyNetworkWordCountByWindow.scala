package com.snail.demo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * 比如说：每10秒钟，把过去30秒的数据进行WordCount
 */
object MyNetworkWordCountByWindow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)    
    
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]") 
    //两个参数：1、conf参数    2、采样时间间隔:每隔3秒
    val ssc = new StreamingContext(conf,Seconds(3)) 
    
    //创建一个DStream，从netcat服务器接收数据
    val lines = ssc.socketTextStream("192.168.112.10", 1234, StorageLevel.MEMORY_ONLY)

    //分词，每个单词记一次数
    val words = lines.flatMap(_.split(" ")).map((_,1))
    
    //比如说：每10秒钟，把过去30秒的数据进行WordCount  -------> 出错
    //正确：  每9秒钟，把过去30秒的数据进行WordCount
    /*
     * 参数：reduceFunc表示要进行的操作是什么
     * 参数：windowDuration是窗口的大小
     * 参数：slideDuration窗口滑动的距离
     * words.reduceByWindow(reduceFunc, windowDuration, slideDuration)
     * 
     * 咱们现在是<key,value>的数据
     */
    val result = words.reduceByKeyAndWindow((x:Int,y:Int)=>(x + y), Seconds(30), Seconds(10))
    
    //打印出来
    result.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}














