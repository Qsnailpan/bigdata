package com.demo.hadoop.mr.order;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Hunter
 * @date 2018年10月26日 下午9:10:14
 * @version 1.0
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
	
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,
			Context context)
			throws IOException, InterruptedException {
		//输出
		context.write(key, NullWritable.get());
	}
}
