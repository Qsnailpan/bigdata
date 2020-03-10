package com.demo.hadoop.hdfs05;

/**
 * @author: create by lipan
 * @version: v1.0
 * @description: TODO:(简单描述下:)
 * @date:2020年3月10日
 * 
 *                  思路？ 接口设计
 */
public interface Mapper {
	// 通用方法
	public void map(String line, Context context);

}
