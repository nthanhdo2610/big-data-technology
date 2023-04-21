package miu.edu.bdt.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveConfig {

private static final Logger logger = LoggerFactory.getLogger(HiveConfig.class);
	
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static final String HOST_HIVE = "quickstart.cloudera";
	private static final String PORT_HIVE = "10000";
	
	private static Connection connection;
	
	public static Connection getHiveConnection() {
		if (connection == null) {
			try {
				// Set JDBC Hive Driver
				Class.forName(JDBC_DRIVER_NAME);
				
				// jdbc:hive2://<hiveserver>:10000/;ssl=false
				StringBuilder config = new StringBuilder();
//				config.append("jdbc:hive2://localhost:10000/default");
//					.append(HOST_HIVE)
//					.append(":")
//					.append(PORT_HIVE)
//					.append("/default");
				
				config.append("jdbc:hive2://")
						.append(HOST_HIVE)
						.append(":")
						.append(PORT_HIVE)
						.append("/;ssl=false");
				
				// Connect to Hive
				// Choose a user that has the rights to write into /user/hive/warehouse/
				// (e.g. hdfs)
				connection = DriverManager.getConnection(config.toString(), "hdfs", "");
			} catch (Exception e) {
				logger.error("Cannot create Hive connection. " + e);
				System.exit(0);
			}
		}
		
		return connection;
	}
	
	public static Statement createStatement() {
		try {
			return connection.createStatement();
		} catch (SQLException e) {
			logger.error("Cannot create Hive connection. " + e);
			System.exit(0);
		}
		return null;
	}
	
}
