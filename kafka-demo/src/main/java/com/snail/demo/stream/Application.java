package com.snail.demo.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * @author lipan 2019年1月9日
 * @description （ 简单的描述 下：数据清洗 "-" ）
 *
 */
public class Application {

	public static void main(String[] args) {
		// 1. 定义两个主题
		String topic1 = "t1";
		String topic2 = "t2";
		// 2. 设置属性
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.112.10:9092");
		// 3.实例
		StreamsConfig conf = new StreamsConfig(prop);
		// 4. 流计算拓扑
		Topology topology = new Topology();
		// 5. 定义kafka 组件数据源
		topology.addSource("Source", topic1)
		.addProcessor("Processor", new ProcessorSupplier<byte[], byte[]>() {
			public Processor<byte[], byte[]> get() {
				return new LogProcessor();
			}
		}, "Source")
		//  数据到哪去
		.addSink("Sink", topic2, "Processor");
		
		// 6. 实例化kafkaStream
		KafkaStreams kafkaStreams = new KafkaStreams(topology, prop);
		kafkaStreams.start();
	}
}
