package com.demo.sougou.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SougouLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	/**
	 * 数据：20111230000005 57375476989eea12893c0c3811607bcf 奇艺高清 1 1 http://www.qiyi.com/ 
	 * 
	 * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");

		// 过滤不满足条件的数据
		if (line.length != 6)
			return;

		// 搜索结果排名第一， 点击排名第二
		if (Integer.valueOf(line[3]) == 1 && Integer.valueOf(line[4]) == 2) {
			context.write(value, NullWritable.get());
		}
	}
}

