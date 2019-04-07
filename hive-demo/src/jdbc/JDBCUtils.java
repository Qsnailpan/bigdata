package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * 工具类
 * 1、获取Connection
 * 2、释放资源
 */
public class JDBCUtils {

	// 定义Hive的Driver
	private static String driver = "org.apache.hive.jdbc.HiveDriver";
	// Oracle: oracle.jdbc.OracleDriver

	// URL地址
	private static String url = "jdbc:hive2://192.168.157.111:10000/default";

	// 注册Driver
	static {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 获取链接
	public static Connection getConnection() {
		try {
			return DriverManager.getConnection(url);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	// 释放资源
	// 问题：能不能通过Java代码去干预GC？
	public static void release(Connection conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				rs = null;
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				st = null;
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				conn = null;
			}
		}
	}

}
