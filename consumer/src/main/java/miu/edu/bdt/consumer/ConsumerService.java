package miu.edu.bdt.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

public class ConsumerService {

    private static Connection connection;
    private static Statement statement;
    private static ConsumerService INSTANCE = null;
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.HIVE_TIMESTAMP_FORMAT);

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    public static ConsumerService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ConsumerService();
        }
        return INSTANCE;
    }

    private ConsumerService() {

        // jdbc:hive2://<hiveserver>:10000/;ssl=false
        String config = "jdbc:hive2://" +
                Constant.HOST_HIVE +
                ":" +
                Constant.PORT_HIVE +
                "/;ssl=false";
        try {
            // Set JDBC Hive Driver
            Class.forName(Constant.JDBC_DRIVER_NAME);

            // Connect to Hive
            // Choose a user that has the rights to write into /user/hive/warehouse/
            // (e.g. hdfs)
            connection = DriverManager.getConnection(config, "hdfs", "");
        } catch (Exception e) {
            logger.error("Cannot create Hive connection. " + e);
            e.printStackTrace();
            System.exit(0);
        }
        try {
            statement = connection.createStatement();

            String drop = String.format(Constant.DROP_TABLE_SQL, Constant.TABLE_NAME);
            System.out.println("DROP_TABLE_SQL: " + drop);
//            statement.execute(drop);

            String sql = String.format(Constant.CREATE_WEATHER_TABLE_SQL, Constant.TABLE_NAME);
            System.out.println("CREATE_WEATHER_TABLE_SQL: " + sql);
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
        String updatedDate = simpleDateFormat.format(new Date());
        try {
            StringJoiner joiner = new StringJoiner(",");
            records.stream().map(r -> String.format("(\"%s\",%.2f,\"%s\")",
                    r.getZipcode(),
                    r.getTemp(),
                    updatedDate)).forEach(joiner::add);
            String sql = String.format(Constant.INSERT_WEATHER_TABLE_SQL, Constant.TABLE_NAME, joiner);
            System.out.println("INSERT_WEATHER_TABLE_SQL: " + sql);
            statement.execute(sql);
        } catch (SQLException e) {
            logger.error("Cannot create Hive connection. " + e);
            System.exit(0);
        }
    }
}
