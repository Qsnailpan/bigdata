package com.snail.demo.sql

import org.apache.spark.{SparkConf, SparkContext}

object sqlUdf {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("sqlJsonText")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.udf.register("strlen", (input: String) => input.length)

    val personInfo = sqlContext.read.json("hdfs://master:9000/person_info.json")

    personInfo.registerTempTable("personInfo")

    sqlContext.sql("SELECT id, name, strlen(name), age FROM personInfo").show()

    println(personInfo.schema)
  }
}
