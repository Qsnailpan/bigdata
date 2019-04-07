package com.snail.demo.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author lipan
 * @date 2018年12月14日 下午9:55:09
 * @version 1.0
 */
public class Consumer1 {
	public static void main(String[] args) {
		// 1.配置消费者属性
		Properties prop = new Properties();
		// 配置属性
		// 服务器地址指定
		prop.put("bootstrap.servers", "192.168.112.10:9092");
		// 配置消费者组
		prop.put("group.id", "g1");
		// 配置是否自动确认offset
		prop.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔
		prop.put("auto.commit.interval.ms", "1000");
		// 序列化
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// 2.实例消费者
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

		// 4.释放资源 线程安全
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				if (consumer != null) {
					consumer.close();
				}
			}
		}));

		// 订阅消息主题
		consumer.subscribe(Arrays.asList("mytopic"));

		// 3.拉消息 推push 拉poll
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			// 遍历消息
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}

		}
	}
}
