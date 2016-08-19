package com.sxt.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJDBCClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://n1:10000/default", "root", "");
			Statement statement = con.createStatement();
			String tableName = "psn1";
			statement.execute("drop table if exists " + tableName);
			statement.execute("create table " + tableName
					+ " (key int, value string)");
			String sql = "show tables '" + tableName + "'";
			System.out.println("Running:"+ sql);
			ResultSet res = statement.executeQuery(sql);
			while(res.next()){
				System.out.println(res.getString(1));
			}
			System.out.println("=========================");
			sql="desc "+ tableName;
			res = statement.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
