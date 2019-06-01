package org.training.hadoop.hive;

import java.sql.*;

/**
 * Created by qianxi.zhang on 6/7/17.
 */
public class HiveExample {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public void process()
		throws SQLException {
		try {
			Class.forName(driverName);
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Connection con = DriverManager.getConnection("jdbc:hive2://hadoop:10000/db1", "bigdata", "bigdata");
		
		Statement stmt = con.createStatement();
		long startTime = System.currentTimeMillis();
		String sql = "show tables";
		String sql2 = "select province, sum(price) as totalPrice\n"
			+ "from record join user_dimension on record.uid=user_dimension.uid group by province\n" + "order by totalPrice desc";
		ResultSet res = stmt.executeQuery(sql2);
		int count = 0;
		while (res.next()) {
			count++;
			System.out.println(res.getString(1) + ":" + res.getInt(2));
		}
		long stopTime = System.currentTimeMillis();
		System.out.println("time: " + (stopTime - startTime) + ", count : " + count);
	}
	
	public static void main(String[] args)
		throws SQLException {
		HiveExample example = new HiveExample();
		example.process();
	}
}
