package com.demo.hadoop.mr.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Hunter
 * @date 2018年10月26日 下午8:44:01
 * @version 1.0
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		//1.获取每行数据
		String line = value.toString();
		
		//2.切分数据
		String[] fields = line.split("\t");
		
		//3.取出字段
		Integer order_id = Integer.parseInt(fields[0]);
		Double price = Double.parseDouble(fields[2]);
		OrderBean ob = new OrderBean(order_id, price);
		
		//4.输出
		context.write(ob, NullWritable.get());
	}
}
