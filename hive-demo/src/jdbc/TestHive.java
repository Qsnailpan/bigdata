package jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestHive {

	public static void main(String[] args) {
		// 查询: select * from emp1;
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = JDBCUtils.getConnection();
			st = conn.createStatement();
			// 执行SQL
			rs = st.executeQuery("select * from emp1");
			while (rs.next()) {
				String ename = rs.getString("ename");
				System.out.println(ename);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			// 释放资源
			JDBCUtils.release(conn, st, rs);
		}
	}
}
