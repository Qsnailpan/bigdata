package com.demo.hadoop.mr.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.获取job信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.获取jar包
		job.setJarByClass(OrderDriver.class);

		// 3.获取mapper与reducer
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// 4.定义mapper输出类型
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5.定义reducer输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6.设置reducer端的分组
		job.setGroupingComparatorClass(OrderGroupingComparator.class);

		// 7.设置分区
		job.setPartitionerClass(OrderPartitioner.class);

		// 8.设置reduceTask个数
		job.setNumReduceTasks(3);

		// 9.设置数据的输入 与 输出
		FileInputFormat.setInputPaths(job, new Path("c://in1026"));
		FileOutputFormat.setOutputPath(job, new Path("c://out1026"));

		// 10:提交任务
		boolean rs = job.waitForCompletion(true);

		System.exit(rs ? 0 : 1);

	}
}
