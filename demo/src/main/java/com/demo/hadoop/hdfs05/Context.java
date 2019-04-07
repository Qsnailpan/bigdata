package com.demo.hadoop.hdfs05;
/**
 * @author Hunter
 * @date 2018年10月17日 下午9:56:21
 * @version 1.0
 * 
 * 思路？
 * 数据传输的类
 * 封装数据
 * 集合
 * <单词,1>
 */

import java.util.HashMap;

public class Context {
	// 数据封装
	private HashMap<Object, Object> contextMap = new HashMap<>();

	// 写数据
	public void write(Object key, Object value) {
		// 放数据到map中
		contextMap.put(key, value);
	}

	// 定义根据key拿到值方法
	public Object get(Object key) {
		return contextMap.get(key);
	}

	// 拿到map当中的数据内容
	public HashMap<Object, Object> getContextMap() {
		return contextMap;
	}
}
