package com.snail.demo.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @author lipan
 * @date 2018年12月14日 下午9:43:59
 * @version 1.0
 */
public class Patition1 implements Partitioner {

	// 设置
	public void configure(Map<String, ?> configs) {
	}

	// 分区逻辑
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		return 1;
	}
	// 释放资源
	public void close() { }

}
