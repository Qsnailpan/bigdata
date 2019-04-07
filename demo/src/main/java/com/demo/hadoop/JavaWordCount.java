package com.demo.hadoop;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * 使用spark submit提交
 * bin/spark-submit --master spark://bigdata111:7077 --class demo.JavaWordCount /root/temp/demo2.jar hdfs://bigdata111:9000/input/data.txt
 */

public class JavaWordCount {
	public static void main(String[] args) {
		// 运行在本地模式，可以设置断点
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		// 运行在集群模式
		// SparkConf conf = new SparkConf().setAppName("JavaWordCount");

		// 创建一个SparkContext对象： JavaSparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 读入HDFS的数据
		JavaRDD<String> rdd1 = sc.textFile("hdfs://192.168.112.10:9000/input/data.txt");
		/*
		 * 分词 FlatMapFunction：接口，用于处理分词的操作 泛型:String 读入的每一句话 U: 返回值 ---> String 单词
		 */
		JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String input) throws Exception {
				// 数据: I love Beijing
				// 分词
				return Arrays.asList(input.split(" ")).iterator();
			}
		});
		/*
		 * 每个单词记一次数 (k2 v2) Beijing ---> (Beijing,1) 参数： String：单词 k2 v2不解释
		 */
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// 执行Reduce的操作
		JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				// 累加
				return a + b;
			}
		});
		// 执行计算(Action)，把结果打印在屏幕上
		List<Tuple2<String, Integer>> result = rdd4.collect();

		for (Tuple2<String, Integer> tuple : result) {
			System.out.println(tuple._1 + "\t" + tuple._2);
		}
		// 停止JavaSparkContext对象
		sc.stop();
	}
}
