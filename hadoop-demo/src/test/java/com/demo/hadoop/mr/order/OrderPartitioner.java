package com.demo.hadoop.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {

		return (key.getOrder_id() & Integer.MAX_VALUE) % numPartitions;
	}

}
