package com.snail.demo.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object sqlTest2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SQLTest")
      .master("local[*]")
      .getOrCreate()

    val lines = session.sparkContext.textFile("hdfs://master:9000/sql_stu.data")

    val stuRDD = lines.map(line =>{
      val fields = line.split(",")
      val no = fields(0).toLong
      val name = fields(1).toString
      val sex = fields(2).toInt
      val age = fields(3).toInt
      val cla = fields(4).toInt
      Row(no, name, sex, age, cla)
    })

    val StudentSchema: StructType = StructType(List(
      StructField("Sno", LongType, nullable = true),
      StructField("Sname", StringType, nullable = true),
      StructField("Ssex", IntegerType, nullable = true),
      StructField("Sbirthday", IntegerType, nullable = true),
      StructField("SClass", IntegerType, nullable = true)
    ))

    val df: DataFrame = session.createDataFrame(stuRDD, StudentSchema)

    import session.implicits._

    val df2 = df.where($"Sno" > 104).orderBy($"SClass")

    df2.show()

    session.stop()

  }
}
