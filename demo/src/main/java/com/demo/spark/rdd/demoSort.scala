package com.demo.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object demoSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("rdd demo")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(List("tom,18,55", "mike,18,60", "mary,19,57"))
    val rdd2 = rdd1.map(line => {
      val gril = line.split(",");
      new Gril(gril(0), gril(1).toInt, gril(2).toInt)
    })
    rdd2.sortBy(s => s).collect().foreach(println)
  }
}
class Gril(val name: String, val age: Int, val weigth: Int) extends Ordered[Gril] with Serializable {
  override def compare(that: Gril): Int = {
     // 年龄相同按体重倒序排s
    if (this.age == that.age) {
      -(this.weigth - that.weigth)
    } else {
      // 按年龄倒序排
      -1
    }
  }
  override def toString = name + "," + age + "," + weigth
}