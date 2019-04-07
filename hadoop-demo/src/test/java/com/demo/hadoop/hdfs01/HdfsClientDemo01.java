package com.demo.hadoop.hdfs01;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
public class HdfsClientDemo01 {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		//1.客户端加载配置文件
		Configuration conf = new Configuration();
		
		//2.指定配置(设置成2个副本数)
		conf.set("dfs.replication", "2");
		
		//3.指定块大小
		conf.set("dfs.blocksize", "64m");
		
		//4.构造客户端
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.50.183:9000/"), 
				conf, "root");
		
		//5.上传文件
		fs.copyFromLocalFile(new Path("c:/words.txt"), new Path("/words222111.txt"));
		
		//6.关闭资源 winUtils
		fs.close();
	}
}
