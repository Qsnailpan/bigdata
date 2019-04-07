package com.demo.hadoop.mr.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Hunter
 * @date 2018年10月29日 下午10:17:02
 * @version 1.0
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		TableBean v = new TableBean();
		Text k = new Text();

		// 区分两张表
		FileSplit imputSplit = (FileSplit) context.getInputSplit();
		String name = imputSplit.getPath().getName();

		// 获取数据
		String line = value.toString();

		// 区分 此时是订单表
		if (name.contains("order.txt")) {
			// 切分字段
			String[] fields = line.split("\t");

			// 封装对象
			v.setOrder_id(fields[0]);
			v.setPid(fields[1]);
			v.setAmount(Integer.parseInt(fields[2]));
			v.setPname("");
			v.setFlag("0");

			// 设置k 商品id作为k
			k.set(fields[1]);

		} else {// 此时为商品表
				// 切分字段
			String[] fields = line.split("\t");

			// 封装对象
			v.setOrder_id("");
			v.setPid(fields[0]);
			v.setAmount(0);
			v.setPname(fields[1]);
			v.setFlag("1");

			// 设置k 商品id
			k.set(fields[0]);

		}

		context.write(k, v);
	}
}
