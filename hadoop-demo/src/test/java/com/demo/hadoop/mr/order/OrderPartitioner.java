package com.demo.hadoop.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Hunter
 * @date 2018年10月26日 下午8:53:18
 * @version 1.0
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		
		return (key.getOrder_id() & Integer.MAX_VALUE) % numPartitions;
	}

}
