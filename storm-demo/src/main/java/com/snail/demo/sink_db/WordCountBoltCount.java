package com.snail.demo.sink_db;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBoltCount extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> map = new HashMap();
	private OutputCollector collector;

	public void execute(Tuple input) {
		// 1.获取数据
		String word = input.getStringByField("word");
		Integer sum = input.getIntegerByField("sum");

		// 2.业务处理
		if (map.containsKey(word)) {
			// 之前出现几次
			Integer count = map.get(word);
			// 已有的
			map.put(word, count + sum);
		} else {
			map.put(word, sum);
		}

		// 3.打印控制台
		System.err.println(Thread.currentThread().getName() + "\t 单词为：" + word + "\t 当前已出现次数为：" + map.get(word));
		collector.emit(new Values(word, map.get(word) + ""));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "total"));

	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}
