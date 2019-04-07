package com.demo.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author Hunter
 * @date 2018年10月19日 下午10:29:55
 * @version 1.0
 */
public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1.获取job信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2.获取jar包
		job.setJarByClass(WordCountDriver.class);
		
		//3.获取自定义的mapper与reducer类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//4.设置map输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5.设置reduce输出的数据类型（最终的数据类型）
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//6.设置输入存在的路径与处理后的结果路径
		FileInputFormat.setInputPaths(job, new Path("c:/in1019/"));
		FileOutputFormat.setOutputPath(job, new Path("c:/out1019/"));
		
		//7.提交任务
		boolean rs = job.waitForCompletion(true);
		System.out.println(rs?0:1);
		
	}
}
