package com.demo.hadoop.hdfs02;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: create by lipan
 * @version: v1.0
 * @description: TODO:(简单描述下: hdfs api 操作示例 demo)
 * @date:2020年3月10日
 */

public class HdfsClientTest {

	FileSystem fs = null;

	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {

		// 1.加载配置
		Configuration conf = new Configuration();

		// 2.设置副本数
		conf.set("dfs.replication", "2");

		// 3.设置块大小
		conf.set("dfs.blocksize", "64m");

		// 4.构造客户端
		fs = FileSystem.get(new URI("hdfs://192.168.205.12:9000/"), conf, "root");

	}

	/*
	 * 在hdfs中创建文件夹 hdfs dfs -mkdir /文件名
	 */
	@Test
	public void hdfsMkdir() throws IllegalArgumentException, IOException {
		// 1.调用创建文件夹方法
		fs.mkdirs(new Path("/mkdir_tmp"));
		// 2.关闭资源
		fs.close();
	}

	/*
	 * 在hdfs中 移动/修改 文件
	 */
	@Test
	public void hdfsRename() throws IllegalArgumentException, IOException {
		// 1.调用移动并修改
		fs.rename(new Path("/README.md"), new Path("/mkdir_tmp/README_tmp.md"));
		// 2.关闭资源
		fs.close();
	}

	/*
	 * 查询hdfs下指定的目录信息
	 */
	@Test
	public void hdfsLs() throws FileNotFoundException, IllegalArgumentException, IOException {
		// 1.调用方法 返回远程迭代器
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/mkdir_tmp/README_tmp.md"), true);

		// 2.取迭代器数据
		while (iter.hasNext()) {
			// 拿数据
			LocatedFileStatus status = iter.next();
			System.out.println("文件的路径为：" + status.getPath());
			System.out.println("块大小为" + status.getBlockSize());
			System.out.println("文件长度为：" + status.getLen());
			System.out.println("副本数量为：" + status.getReplication());
			System.out.println("块信息为：" + Arrays.toString(status.getBlockLocations()));

			System.out.println("===============================");
		}

		// 3.关闭资源
		fs.close();

	}

	/*
	 * 判断文件还是文件夹
	 */

	@Test
	public void findAtHdfs() throws FileNotFoundException, IllegalArgumentException, IOException {
		// 1.展示状态信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		// 2.遍历所有文件
		for (FileStatus ls : listStatus) {
			if (ls.isFile()) {
				// 文件
				System.out.println("文件----f----" + ls.getPath().getName());
			} else {
				// 文件夹
				System.out.println("文件----d----" + ls.getPath().getName());
			}

		}
	}

	/*
	 * 在hdfs中删除文件夹 hdfs dfs -rm -r /
	 */
	@Test
	public void hdfsRm() throws IllegalArgumentException, IOException {
		// fs.delete(new Path("/hunterhenshuai"));
		// 1.调用删除文件方法 参数1：删除的路径 参数2：是否递归删除
		fs.delete(new Path("/mkdir_tmp"), true);
		fs.close();
	}

}
