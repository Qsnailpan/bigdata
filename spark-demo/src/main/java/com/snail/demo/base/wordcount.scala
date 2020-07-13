package com.snail.demo.base
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/root/codes/mapreduce_wordcount_python/The_Man_of_Property.txt")
    // val out_rdd = rdd.flatMap(_.split(" "))
    val out_rdd = rdd.flatMap{ x =>
      x.split(" ")
    }.map { x =>
      (x, 1)
    }.reduceByKey(_+_).map { x =>
      x._1 + "\t" + x._2.toString
    }saveAsTextFile("/root/codes/spark_data/wordcount_output")

//    println(out_rdd.foreach(println(_)))
  }
}
