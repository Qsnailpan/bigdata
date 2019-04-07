package com.snail.demo.sink_db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.storm.jdbc.common.ConnectionProvider;

/**
 * @author lipan 2019年1月20日
 * @description （ 简单的描述 下：TODO ）
 *
 */
public class MyConnectionProvider implements ConnectionProvider {
	private static String driver = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql:///test";
	private static String username = "root";
	private static String pwd = "root";

	static {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, username, pwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}

	public void cleanup() {
	}

	public void prepare() {
	}
}
