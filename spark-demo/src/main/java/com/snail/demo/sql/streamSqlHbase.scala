package com.snail.demo.sql

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object HbaseHandler {
  def insert(row: String, column: String, value: String) {
    // Hbase配置
    val tableName = "sparkstream_kafkahbase_table" // 定义表名
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val hTable = new HTable(hbaseConf, tableName)
    val thePut = new Put(row.getBytes)
    thePut.add("info".getBytes,column.getBytes,value.getBytes)
    hTable.setAutoFlush(false, false)
    // 写入数据缓存
    hTable.setWriteBufferSize(3*1024*1024)
    hTable.put(thePut)
    // 提交
    hTable.flushCommits()
  }
}

object streamSqlHbase {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: streamSqlHbase <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("streamSqlHbase")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))

    val lines = ssc.socketTextStream(
      args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val wordsDataFrame = rdd.map{ x=>
        (x.split(" ")(0),x.split(" ")(1),x.split(" ")(2))
      }.map(w => (w._1, w._2, w._3)).toDF("key", "col", "val")

      wordsDataFrame.registerTempTable("words")
      val wordCountsDataFrame =
        sqlContext.sql("select key, col, val from words")
      println(s"========= $time =========")
      wordCountsDataFrame.show()


      wordCountsDataFrame.foreach(x => HbaseHandler.insert(
        x.getAs[String]("key"),
        x.getAs[String]("col"),
        x.getAs[String]("val")))

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
