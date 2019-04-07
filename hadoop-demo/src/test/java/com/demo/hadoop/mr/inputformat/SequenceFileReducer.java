package com.demo.hadoop.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Hunter
 * @date 2018年10月27日 下午10:02:22
 * @version 1.0
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		for(BytesWritable v:values) {
			context.write(key, v);
		}
		
		
	}
}
