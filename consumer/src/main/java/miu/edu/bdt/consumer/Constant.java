package miu.edu.bdt.consumer;

public class Constant {

    public static final String KAFKA_BROKERS = "quickstart.cloudera:9092";
    public static final String TOPIC_NAME = "weather_topic";
    public static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    public static final String JDBC_HIVE_CONNECTION = "jdbc:hive2://quickstart.cloudera:10000/;ssl=false";
    public static final String TABLE_NAME = "bdt_weather";
    public static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s";
    public static final String CREATE_WEATHER_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s (zip_code STRING,city STRING,temperature FLOAT,updated_date TIMESTAMP) STORED AS PARQUET";
    public static final String INSERT_WEATHER_TABLE_SQL = "INSERT INTO %s VALUES  %s";

}
