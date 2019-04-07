package com.demo.hadoop.mr.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Hunter
 * @date 2018年10月26日 下午8:31:59
 * @version 1.0
 */
public class OrderBean implements WritableComparable<OrderBean> {

	// 定义属性
	private int order_id;// 定义订单id
	private double price;// 价格

	public OrderBean() {
		super();

	}

	public OrderBean(int order_id, double price) {
		super();
		this.order_id = order_id;
		this.price = price;
	}

	public int getOrder_id() {
		return order_id;
	}

	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	// 序列化
	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id);
		out.writeDouble(price);
	}

	// 反序列化
	public void readFields(DataInput in) throws IOException {
		order_id = in.readInt();
		price = in.readDouble();
	}

	@Override
	public String toString() {
		return order_id + "\t" + price;
	}

	// 排序 需要比较id 再比较价格
	public int compareTo(OrderBean o) {

		int rs;

		// 根据id排序
		if (order_id > o.order_id) {
			// id大的往下排
			rs = 1;
		} else if (order_id < o.order_id) {
			// id小的往上排
			rs = -1;
		} else {
			// id相等 价格高的上排
			rs = price > o.getPrice() ? -1 : 1;
		}

		return rs;
	}

}
