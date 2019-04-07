package com.snail.demo.sink_db;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class WordCountDriver {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();

		// 2.指定设置
		builder.setSpout("WordCountSpout", new WordCountSpout(), 4);
		builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt(), 6).setNumTasks(4)
				.shuffleGrouping("WordCountSpout");
		builder.setBolt("WordCountBolt", new WordCountBoltCount(), 4).shuffleGrouping("WordCountSplitBolt");

		// 集成HDFS
		// builder.setBolt("WordCount_hdfs",
		// createHDFSBolt()).shuffleGrouping("WordCountBolt");

		// 集成HBase 加入hbase 配置
		// builder.setBolt("WordCount_hbase", createHBaseBolt(),
		// 1).shuffleGrouping("WordCountBolt");

		// Map<String, Object> hbConf = new HashMap<String, Object>();
		// hbConf.put("hbase.rootdir", "hdfs://192.168.112.10:9000/hbase");
		// hbConf.put("hbase.zookeeper.quorum", "192.168.112.10:2181");
		// conf.put("hbase.conf", hbConf);

		// 集成redis
		// builder.setBolt("WordCount_redis",
		// createRedisBolt(),1).shuffleGrouping("WordCountBolt");

		// 集成jdbc
		builder.setBolt("WordCount_jdbc", createJdbcBolt(), 1).shuffleGrouping("WordCountBolt");

		conf.setNumWorkers(4);

		// 4.提交任务
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("wordcounttopology", conf, builder.createTopology());

		// 集群模式运行
		// StormSubmitter.submitTopology("wordcounttopology", conf,
		// builder.createTopology());
	}

	private static IRichBolt createJdbcBolt() {
		ConnectionProvider provider = new MyConnectionProvider();
		SimpleJdbcMapper simpleJdbcMapper = new SimpleJdbcMapper("wordcount", provider);
		return new JdbcInsertBolt(provider, simpleJdbcMapper).withTableName("wordcount");
	}

	/**
	 * 集成 redis
	 * 
	 * @return
	 */
	private static IRichBolt createRedisBolt() {
		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost("192.168.112.10").setPort(6379).build();
		return new RedisStoreBolt(poolConfig, new WordCountRedisStoreMapper());
	}

	public static class WordCountRedisStoreMapper implements RedisStoreMapper {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private RedisDataTypeDescription description;
		private final String hashKey = "wordCount";

		public WordCountRedisStoreMapper() {
			description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
		}

		public RedisDataTypeDescription getDataTypeDescription() {
			return description;
		}

		public String getKeyFromTuple(ITuple tuple) {
			return tuple.getStringByField("word");
		}

		public String getValueFromTuple(ITuple tuple) {
			return tuple.getStringByField("total");
		}
	}

	private static IRichBolt createHBaseBolt() {
		// 集成 HBase
		SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField("word").withColumnFamily("info");
//		 .withColumnFields(new Fields("word"))
//		 .withColumnFields(new Fields("total"));
		HBaseBolt hbase = new HBaseBolt("result", mapper).withConfigKey("hbase.conf");
		return null;
	}

	/**
	 * 集成HDFS
	 * 
	 * @return
	 */
	private static IRichBolt createHDFSBolt() {
		// 指定用户
		System.setProperty("HADOOP_USER_NAME", "root");
		// 写入时 数据格式化 |
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

		// 输出的tuple达到1000个就跟hdfs同步一次（减少IO）
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// 每 5M 的数据生成一个文件
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
		// hdfs 保存目录
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo/");

		HdfsBolt bolt = new HdfsBolt().withFsUrl("hdfs://192.168.112.10:9000").withFileNameFormat(fileNameFormat)
				.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
		return (IRichBolt) bolt;
	}

}
