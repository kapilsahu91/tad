package com.afour.tad.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnection {
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = "jdbc:mysql://localhost/test";
	private static final String USER = "root";
	private static final String PASS = "root";
	private static Connection connection = null;
	
	public static Connection getConnection() {
		if(connection == null){
			try{
				Class.forName(JDBC_DRIVER);
				connection = DriverManager.getConnection(DB_URL, USER, PASS);
			}catch(SQLException|ClassNotFoundException e){
				e.printStackTrace();
			}
			return connection;
		}
		else return connection;

	}

}
