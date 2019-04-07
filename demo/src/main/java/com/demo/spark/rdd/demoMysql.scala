package com.demo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement

object demoMysql {

  // 数据格式：
  //  192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/ HTTP/1.1" 200 259
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("rdd demo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\BigData\\data\\input\\localhost_access_log.2017-07-30.txt", 2)

    rdd1.map(line => {
      var strs = line.split(" ")
      (strs(6), 1)
    })
      .reduceByKey(_ + _) // 按key计数
      .sortBy(_._2, false) // 按数量（_2）降序
      .take(3) // 取前三个
      .foreach(x => {

        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "root");
        val sql = "INSERT INTO PAGECOUNT(PAGE,COUNT) VALUES (?,?)"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        ps.setString(1, x._1)
        ps.setInt(2, x._2)
        ps.executeUpdate()
        
        conn.close()
      })
  }
}
//  结果
//(/MyDemoWeb/oracle.jsp,9)
//(/MyDemoWeb/hadoop.jsp,9)
//(/MyDemoWeb/mysql.jsp,3)