package com.demo.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap

object myPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("My Web Count Example").setMaster("local")
    val sc = new SparkContext(conf)
    //读入数据，并切分数据:
    // 192.168.88.1 - - [30/Jul/2017:12:54:38 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242
    val rdd1 = sc.textFile("E:\\BigData\\data\\input\\localhost_access_log.2017-07-30.txt").map(
      line => {
        // /MyDemoWeb/hadoop.jsp
        var strs = line.split(" ")
        //  hadoop.jsp
        val page: String = strs(6).substring(strs(6).lastIndexOf("/"))
        (page, line)
      })
    // key 去重
    val rdd2 = rdd1.keys.distinct()
    // 创建分区器
    var part = new myPartition(rdd2.collect)
    // 自定义分区输出
    rdd1.partitionBy(part).saveAsTextFile("E:\\BigData\\data\\out3")
  }
}
//定义自己的分区器
class myPartition(allJSPNames: Array[String]) extends Partitioner {
  val partitionsMap = new HashMap[String, Int]()
  var parID = 0
  for (name <- allJSPNames) {
    partitionsMap.put(name, parID)
    parID += 1
  }
  override def getPartition(key: Any): Int = partitionsMap.getOrElse(key.toString, 0)
  override def numPartitions: Int = partitionsMap.size
} 

// 结果输出

//part-00000
//(/,192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/ HTTP/1.1" 200 259)
//part-00001
//(/hadoop.jsp,192.168.88.1 - - [30/Jul/2017:12:54:38 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242)
//(/hadoop.jsp,192.168.88.1 - - [30/Jul/2017:12:54:40 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242)
// ...






