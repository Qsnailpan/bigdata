package com.demo.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<IntWritable, Text> {

	public int getPartition(IntWritable k2, Text v2, int numTask) {
		return 1;
	}
}
