package com.demo.hadoop.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Hunter
 * @date 2018年10月29日 下午8:51:38
 * @version 1.0
 */
public class FileReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		//数据换行
		String k = key.toString()+"\r\n";
		
		context.write(new Text(k), NullWritable.get());
	}

}
