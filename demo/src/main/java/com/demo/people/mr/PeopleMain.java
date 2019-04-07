package com.demo.people.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(PeopleMain.class);

		job.setMapperClass(PeopleMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PeopleInfo.class);

		job.setReducerClass(PeopleReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.112.10:9000/input/sample_people_info.txt"));
//		FileInputFormat.setInputPaths(job, new Path("E:\\BigData\\data\\input\\sample_people_info.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.112.10:9000/out/sample_people2"));

		job.waitForCompletion(true);
		
		
	}

}
