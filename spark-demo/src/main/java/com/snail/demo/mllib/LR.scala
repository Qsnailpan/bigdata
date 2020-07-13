package com.snail.demo.mllib
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object LR {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("LR")

    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))

    val parsedData = lines.map { x =>
      val parts = x.split("\t")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(',').map(_.toDouble)))
    }

    val model = LinearRegressionWithSGD.train(parsedData, 2, 0.1)
    val result = model.predict(Vectors.dense(1,3))
    println(model.weights)
    println(result)

    val result_1 = model.predict(Vectors.dense(1,1))
    println(model.weights)
    println(result_1)
  }
}
