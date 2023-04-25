package miu.edu.bdt.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;

public class ConsumerService {

    private static Connection connection;
    private static Statement statement;
    private static ConsumerService INSTANCE = null;

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    public static ConsumerService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ConsumerService();
        }
        return INSTANCE;
    }

    private ConsumerService() {

        try {
            // Set JDBC Hive Driver
            Class.forName(Constant.JDBC_DRIVER_NAME);

            // Connect to Hive
            // Choose a user that has the rights to write into /user/hive/warehouse/
            // (e.g. hdfs)
            connection = DriverManager.getConnection(Constant.JDBC_HIVE_CONNECTION, "hdfs", "");
        } catch (Exception e) {
            logger.error("Cannot create Hive connection. " + e);
            e.printStackTrace();
            System.exit(0);
        }
        try {
            statement = connection.createStatement();

//            String drop = String.format(Constant.DROP_TABLE_SQL, Constant.TABLE_NAME);
//            logger.info("DROP_TABLE_SQL: " + drop);
//            statement.execute(drop);

            String sql = String.format(Constant.CREATE_WEATHER_TABLE_SQL, Constant.TABLE_NAME);
            logger.info("CREATE_WEATHER_TABLE_SQL: " + sql);
            statement.execute(sql);

        } catch (SQLException e) {
            logger.error("Cannot create Hive connection. " + e);
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void batchInsert(List<Weather> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            StringJoiner joiner = new StringJoiner(",");
            records.stream().map(r -> String.format("(\"%s\",\"%s\",%.2f,\"%s\")",
                    r.getZipcode(),
                    r.getCity(),
                    r.getTemp(),
                    r.getUpdatedDate())).forEach(joiner::add);
            String sql = String.format(Constant.INSERT_WEATHER_TABLE_SQL, Constant.TABLE_NAME, joiner);
            logger.info("INSERT_WEATHER_TABLE_SQL: " + sql);
            statement.execute(sql);
        } catch (SQLException e) {
            logger.error("Cannot create Hive connection. " + e);
            System.exit(0);
        }
    }
}
