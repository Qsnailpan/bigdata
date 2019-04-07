package com.snail.demo.sink_db;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 
 * 需求：单词计数
 * 
 * 实现接口：IRichSpout IRichBolt 继承抽象类:BaseRichSpout BaseRichSpout 常用
 * 
 */
public class WordCountSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// 定义我们要产生的数据
	private String[] datas = { "I love Beijing", "I love China", "Beijing is the capital of China" };
	// 定义一个变量来保存输出流
	private SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		// 每隔2秒采集一次
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 由Storm的框架调用，用于如何接受数据
		// 产生一个3以内的随机数
		int random = (new Random()).nextInt(3);
		// 数据
		String data = datas[random];

		// 把数据发送给下一个组件
		// 数据一定要遵循schema的结构
		System.out.println("采集的数据是：" + data);
		this.collector.emit(new Values(data));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itstar"));
	}
}
