package com.snail.hbase.hdfsmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lipan
 * @version 1.0, 20:34 2018/12/8
 */
public class EmpDriver implements Tool {
	private Configuration conf = null;

	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create();
	}

	public Configuration getConf() {
		return this.conf;
	}

	public int run(String[] args) throws Exception {
		// 1.创建job
		Job job = Job.getInstance(conf);
		job.setJarByClass(EmpDriver.class);

		// 2.配置mapper
		job.setMapperClass(HdfsMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 3.配置reducer
		TableMapReduceUtil.initTableReducerJob("emp", EmpReducer.class, job);

		// 4.配置输入inputformat
		FileInputFormat.addInputPath(job, new Path("/emp.tsv"));

		// 5.输出
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new EmpDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
