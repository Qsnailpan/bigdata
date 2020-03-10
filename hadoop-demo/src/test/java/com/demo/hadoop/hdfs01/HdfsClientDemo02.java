package com.demo.hadoop.hdfs01;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author: create by lipan
 * @version: v1.0
 * @description: TODO:(简单描述下: hdfs 文件下载到本地磁盘 demo)
 * @date:2020年3月10日
 * 
 *                  注意：1.需要配置hadoop环境 2.需要编译好的winutil包导入bin
 */

public class HdfsClientDemo02 {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

		// 1.加载配置
		Configuration conf = new Configuration();

		// 2.设置副本数
		conf.set("dfs.replication", "2");

		// 3.设置块大小
		conf.set("dfs.blocksize", "64m");

		// 4.构造客户端
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.205.12:9000/"), conf, "root");

		// 5.hdfs数据下载到windows本地
		fs.copyToLocalFile(new Path("/README.md"), new Path("d:/"));

		// 6.关闭资源
		fs.close();
	}
}
