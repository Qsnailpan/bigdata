package com.demo.hadoop.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 * @author Hunter
 * @date 2018年10月27日 下午9:53:51
 * @version 1.0
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

	Text k = new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//1.拿到切片信息
		FileSplit split = (FileSplit)context.getInputSplit();
		
		//2.路径
		Path path = split.getPath();
		
		//3.即带路径又带名称
		k.set(path.toString());
		
	}
	
	
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Context context)
			throws IOException, InterruptedException {
		//输出
		context.write(k, value);
	}

	
}
