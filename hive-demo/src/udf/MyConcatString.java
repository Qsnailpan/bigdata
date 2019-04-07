package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

//实现关系型数据库中concat函数
public class MyConcatString extends UDF {

	// 必须重写一个方法，名字必须叫: evaluate
	public String evaluate(String a, String b) {
		return a + "**************" + b;
	}
}
