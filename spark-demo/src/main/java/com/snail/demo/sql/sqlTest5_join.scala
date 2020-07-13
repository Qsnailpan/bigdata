package com.snail.demo.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object sqlTest5_join {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("SQLTest")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    val lines = session.createDataset(List("1,zhangsan,china", "2,lisi,usa", "3,wangwu,japan", "4,zhaoliu,England"))


    val td = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1).toString
      val nation = fields(2).toString

      (id, name, nation)
    })

    val df1 = td.toDF("id", "name", "nation")
//    df1.show()

    val nations = session.createDataset(List("china,zhongguo", "usa,meiguo", "japan,riben", "England,yingguo"))
    val ndataset = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0).toString
      val cname = fields(1).toString

      (ename, cname)
    })

    val df2 = ndataset.toDF("ename", "cname")

    // method 1
//    df1.createTempView("t_user")
//    df2.createTempView("t_nation")
//    val r = session.sql("select name, cname from t_user join t_nation on t_user.nation == t_nation.ename")

    // method 2
    val r = df1.join(df2, $"nation" === $"ename")

    r.show()

    session.stop()
  }
}
