package com.snail.demo.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

object sqlJsonText {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("sqlJsonText")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val personInfo = sqlContext.read.json("hdfs://master:9000/person_info.json")

    personInfo.registerTempTable("personInfo")

    sqlContext.sql("SELECT id, name, age FROM personInfo").show()

    println(personInfo.schema)
  }
}