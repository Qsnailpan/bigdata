package com.demo.people.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeopleReduce extends Reducer<Text, PeopleInfo, Text, IntWritable> {

	@Override
	protected void reduce(Text key3, Iterable<PeopleInfo> values, Context context)
			throws IOException, InterruptedException {
		int total = 0, maxHeight = 0, minHeight = 0;
		for (PeopleInfo peopleInfo : values) {
			total++;
			if (peopleInfo.getHeight() > maxHeight) {
				maxHeight = peopleInfo.getHeight();
			}

			if (minHeight == 0)
				minHeight = peopleInfo.getHeight();
			if (peopleInfo.getHeight() < minHeight) {
				minHeight = peopleInfo.getHeight();
			}
		}

		System.out.println("性别：" + key3.toString());
		System.out.println("人数：" + total);
		System.out.println("最高身高：" + maxHeight);
		System.out.println("最低身高：" + minHeight);

		// 输出
		context.write(new Text("Total of " + key3.toString()), new IntWritable(total));
		context.write(new Text("Highest of " + key3.toString()), new IntWritable(maxHeight));
		context.write(new Text("Lowest of " + key3.toString()), new IntWritable(minHeight));

	}

}
