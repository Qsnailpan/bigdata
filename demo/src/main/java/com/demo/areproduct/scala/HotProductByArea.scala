package com.demo.areproduct.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object HotProductByArea {
  //定义case class来代表输入的数据
  //地区表
  case class AreaInfo(area_id:String,area_name:String)
  
  //商品表
  case class ProductInfo(product_id:String,product_name:String,marque:String,
                         barcode:String, price:Double,brand_id:String,market_price:Double,stock:Int,status:Int)
                         
  //经过清洗后的用户点击数据   需要从url中解析出product_id
  case class LogInfo(user_id:String,user_ip:String,product_id:String,
                     click_time:String,action_type:String,area_id:String)        
                         
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //设置环境变量
//    System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-2.4.1")
//    System.setProperty("HADOOP_USER_NAME","hdfs")
    
    //创建SparkSession
    val spark = SparkSession.builder().master("local").appName("Project04-HotProductByArea").getOrCreate()
    import spark.sqlContext.implicits._

    //获取地区数据
    val areaDF = spark.sparkContext.textFile("hdfs://192.168.112.10:9000/input/project04/area/areainfo.txt")
                   .map(_.split(",")).map(x => new AreaInfo(x(0),x(1))).toDF
    areaDF.createTempView("area")
    
    //获取商品信息
    //1,nike shoes,UZUSSS,UZUSSS198612252222,20.2,NIKE,25.2,20,0
    val productDF = spark.sparkContext.textFile("hdfs://192.168.112.10:9000/input/project04/product/productinfo.txt")
                   .map(_.split(",")).map(x=> new ProductInfo(x(0),x(1),x(2),x(3),x(4).toDouble,x(5),x(6).toDouble,x(7).toInt,x(8).toInt))
                   .toDF()
    productDF.createTempView("product")
    
    
    //获取用户点击日志并注册成表
    //1,201.105.101.102,http://mystore.jsp/?productid=1,2017020020,1,1 
    val clickLogDF = spark.sparkContext.textFile("hdfs://192.168.112.10:9000/input/project04/clicklog/clean")
                     .map(_.split(",")).map(x => new LogInfo(x(0),x(1),x(2).substring(x(2).indexOf("=")+1),x(3),x(4),x(5)))
                     .toDF()
    clickLogDF.createTempView("clicklog")

    //通过SQL分析各区域商品的热度
    var sql = "select a.area_id,a.area_name,p.product_id,p.product_name,count(c.product_id) PV "
    sql =  sql + "from area a,product p,clicklog c "
    sql = sql + "where a.area_id=c.area_id and p.product_id=c.product_id " 
    sql = sql + "group by a.area_id,a.area_name,p.product_id,product_name"
    
    //将结果输出到屏幕
    spark.sql(sql).show()
   
    //也可以使用下面的方式，将结果保存到HDFS
    //注意：保存的text方法，只支持一个列，所以这时候可以使用concat函数将多个列拼加成一个列
    //如下：
    var sql1 = "select concat(a.area_id,',',a.area_name,',',p.product_id,',',p.product_name,',',count(c.product_id))"
    sql1 =  sql1 + "from area a,product p,clicklog c "
    sql1 = sql1 + "where a.area_id=c.area_id and p.product_id=c.product_id " 
    sql1 = sql1 + "group by a.area_id,a.area_name,p.product_id,product_name"    
//    spark.sql(sql1).repartition(1).write.text("hdfs://192.168.112.10:9000/projectresult/04")
  }
}



















