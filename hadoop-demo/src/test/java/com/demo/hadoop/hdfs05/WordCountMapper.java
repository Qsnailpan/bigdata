package com.demo.hadoop.hdfs05;

/**
 * @author: create by lipan
 * @version: v1.0
 * @description: TODO:(简单描述下:)
 * @date:2020年3月10日
 * 
 *                  思路： 添加一个map方法 单词切分 相同key的value ++
 */
public class WordCountMapper implements Mapper {

	public void map(String line, Context context) {
		// 1.拿到这行数据 切分
		String[] words = line.split(" ");

		// 2.拿到单词 相同的key value++ hello 1 itstar 1
		for (String word : words) {
			Object value = context.get(word);
			if (null == value) {
				context.write(word, 1);
			} else {
				// 不为空
				int v = (Integer) value;
				context.write(word, v + 1);
			}
		}

	}
}
