package com.snail.demo.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author lipan   2019年1月9日 
 * @description    （ 简单的描述 下：数据清洗 ）
 *
 */
public class LogProcessor implements Processor<byte[],byte[]>{

	private ProcessorContext context;
	public void close() { }

	public void init(ProcessorContext context) {
		this.context = context;
	}

	public void process(byte[] key, byte[] val) {
		// 1. 拿到消息数据
		String value = new String(val);
		// 2. 如果包含‘-’，取右侧数据
		if(value.contains("-")) {
			value = value.split("-")[1];
		}
		context.forward(key, value.getBytes());
	}

}
