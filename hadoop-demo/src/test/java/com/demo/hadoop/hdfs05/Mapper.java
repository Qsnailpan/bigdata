package com.demo.hadoop.hdfs05;
/**
 * @author Hunter
 * @date 2018年10月17日 下午9:53:21
 * @version 1.0
 * 
 * 思路？
 * 接口设计
 */
public interface Mapper {
	//通用方法
	public void map(String line,Context context);
	
}
