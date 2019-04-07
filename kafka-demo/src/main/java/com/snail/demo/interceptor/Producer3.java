package com.snail.demo.interceptor;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author lipan
 * @date 2018年12月14日 下午8:46:57
 * @version 1.0
 * 
 *          kafka生产者API
 */
public class Producer3 {

	public static void main(String[] args) {

		// 1.配置生产者属性（指定多个参数）
		Properties prop = new Properties();

		// 参数配置
		// kafka节点的地址
		prop.put("bootstrap.servers", "192.168.112.10:9092");
		// 发送消息是否等待应答
		prop.put("acks", "all");
		// 配置发送消息失败重试
		prop.put("retries", "0");
		// 配置批量处理消息大小
		prop.put("batch.size", "10241");
		// 配置批量处理数据延迟
		prop.put("linger.ms", "5");
		// 配置内存缓冲大小
		prop.put("buffer.memory", "12341235");
		// 消息在发送前必须序列化
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		// 拦截器
		ArrayList<String> inList = new ArrayList<String>();
		inList.add("com.itstare.kafka.interceptor.TimeInterceptor");
		prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, inList);
		
		// 2.实例化producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		// 3.发送消息
		for (int i = 0; i < 99; i++) {
			producer.send(new ProducerRecord<String, String>("mytopic", "lipan-time" + i));
		}

		// 4.释放资源
		producer.close();

	}
}
