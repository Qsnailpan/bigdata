package com.demo.spark.sql

import java.util.Properties

import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

/**
 *  读取文件 通过schema 方式创建DataFrame并将数据保存的 mysql
 *
 *  student.txt:
 *     1 tom 19
 *     2 mike 20
 *     3 mary 21
 */
object Demo {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().master("local").appName("My Demo 1").getOrCreate()

    //从指定的文件中读取数据，生成对应的RDD
    val personRDD = spark.sparkContext.textFile("E:\\BigData\\data\\input\\student.txt").map(_.split(" "))

    //创建schema ，通过StructType
    val schema = StructType(
      List(
        StructField("personID", IntegerType, true),
        StructField("personName", StringType, true),
        StructField("personAge", IntegerType, true)))

    //将RDD映射到Row RDD 行的数据上
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))

    //生成DataFrame
    val personDF = spark.createDataFrame(rowRDD, schema)

    //将DF注册成表
    personDF.createOrReplaceTempView("myperson")

    //执行SQL
    val result = spark.sql("select * from myperson")

    //显示
    //result.show()

    //将结果保存到mysql中
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    // 如果表已存在，覆盖（overwrite） ，可选值： 'overwrite', 'append', 'ignore', 'error'.F
    result.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/spark", "student1", props)

    //停止spark context
    spark.stop()
  }

}