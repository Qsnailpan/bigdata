package com.demo.sougou.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class SougouLogApp {
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();

		System.setProperty("HADOOP_USER_NAME", "root");
		Job job = Job.getInstance(config);
		job.setJarByClass(SougouLogApp.class);

		job.setMapperClass(SougouLogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.112.10:9000/input/SogouQ.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.112.10:9000/out/ss2"));

		job.waitForCompletion(true);

	}

}
