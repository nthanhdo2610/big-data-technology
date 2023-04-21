package miu.edu.bdt.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConfig {

    private static final Logger logger = LoggerFactory.getLogger(HiveConfig.class);
    private static Connection connection;

    public static Connection getHiveConnection() {
        if (connection == null) {
            try {
                // Set JDBC Hive Driver
                Class.forName(Constant.JDBC_DRIVER_NAME);

                // jdbc:hive2://<hiveserver>:10000/;ssl=false
                StringBuilder config = new StringBuilder();
//				config.append("jdbc:hive2://localhost:10000/default");
//					.append(HOST_HIVE)
//					.append(":")
//					.append(PORT_HIVE)
//					.append("/default");

                config.append("jdbc:hive2://")
                        .append(Constant.HOST_HIVE)
                        .append(":")
                        .append(Constant.PORT_HIVE)
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
