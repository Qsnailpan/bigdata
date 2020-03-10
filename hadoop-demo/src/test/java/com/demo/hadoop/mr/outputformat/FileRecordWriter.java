package com.demo.hadoop.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FileRecordWriter extends RecordWriter<Text, NullWritable> {

	Configuration conf = null;
	FSDataOutputStream itstarlog = null;
	FSDataOutputStream otherlog = null;

	// 1.定义数据输出路径
	public FileRecordWriter(TaskAttemptContext job) throws IOException {
		// 获取配置信息
		conf = job.getConfiguration();

		// 获取文件系统
		FileSystem fs = FileSystem.get(conf);

		// 定义输出路径
		itstarlog = fs.create(new Path("c:/outitstaredu/itstar.logs"));// part-r-00000
		otherlog = fs.create(new Path("c:/outputother/other.logs"));
	}

	// 2.数据输出
	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// 判断的话根据key
		if (key.toString().contains("itstar")) {
			// 写出到文件
			itstarlog.write(key.getBytes());
		} else {
			otherlog.write(key.getBytes());
		}

	}

	// 3.关闭资源
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		if (null != itstarlog) {
			itstarlog.close();
		}

		if (null != otherlog) {
			otherlog.close();
		}

	}

}
