package com.demo.hadoop.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Hunter
 * @date 2018年10月29日 下午8:27:13
 * @version 1.0
 */
public class FuncFileOutputFormat extends FileOutputFormat<Text, NullWritable>{

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		FileRecordWriter fileRecordWriter = new FileRecordWriter(job);
		
		return fileRecordWriter;
	}

}
