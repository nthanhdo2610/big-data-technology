package miu.edu.bdt.consumer;

public class Constant {

    public static final String KAFKA_BROKERS = "quickstart.cloudera:9092";
    public static final String TOPIC_NAME = "weather_topic";
    public static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    public static final String HOST_HIVE = "quickstart.cloudera";
    public static final String PORT_HIVE = "10000";
    public static final String TABLE_NAME = "tb_weather";
    public static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s";

}
