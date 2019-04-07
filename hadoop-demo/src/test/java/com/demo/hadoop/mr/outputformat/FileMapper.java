package com.demo.hadoop.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Hunter
 * @date 2018年10月29日 下午8:48:43
 * @version 1.0
 */
public class FileMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//输出
		context.write(value, NullWritable.get());
	}

}
