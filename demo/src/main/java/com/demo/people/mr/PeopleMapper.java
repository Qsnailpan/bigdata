package com.demo.people.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeopleMapper extends Mapper<LongWritable, Text, Text, PeopleInfo> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] words = value.toString().split(" ");

		// 生成PeopleInfo对象
		PeopleInfo info = new PeopleInfo();
		info.setPeopleID(Integer.parseInt(words[0]));
		info.setGender(words[1]);
		info.setHeight(Integer.parseInt(words[2]));

		// System.out.println(info);
		// 输出： key2 性别 value2 info
		context.write(new Text(info.getGender()), info);

	}
}
