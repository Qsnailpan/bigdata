package com.snail.demo.streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object WordCountState {

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCountState <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val wordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunction _)
    wordCounts.print()
    wordCounts.saveAsTextFiles("hdfs://master:9000/stream_stats_out", "doc")
    ssc.start()
    ssc.awaitTermination()
  }
}
