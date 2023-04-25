package miu.edu.bdt.visualizer;

public class Constant {

    public static final String TABLE_NAME = "bdt_weather";
    public static final String THRIFT_CONNECTION = "thrift://quickstart.cloudera:9083";
//    public static final String IOWA_TABLE_NAME = "bdt_weather_iowa";
    public static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS %s";
    public static final String SELECT_WEATHER_IOWA_SQL = "SELECT * FROM %s WHERE zip_code in(\"52099\",\"50614\",\"52204\",\"52243\",\"50398\") ORDER BY updated_date DESC";
//    public static final String SELECT_LIMIT_10_WEATHER_IOWA_SQL = "SELECT * FROM %s WHERE zip_code IN(\"52099\",\"50614\",\"52204\",\"52243\",\"50398\") ORDER BY updated_date DESC LIMIT 10";

}
