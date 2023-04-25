package miu.edu.bdt.visualizer;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkService {

    private static SparkConf sparkConf = null;
    private static SparkSession sparkSession = null;
    private static SparkService INSTANCE = null;

    private SparkService() {
        sparkConf = new SparkConf();

        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", Constant.THRIFT_CONNECTION);
        sparkConf.set("hive.exec.scratchdir", "/tmp/the-current-user-has-permission-to-write-in");

        sparkSession = SparkSession
                .builder()
                .appName("Spark SQL-Hive")
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
    }

    public static SparkService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SparkService();
        }
        return INSTANCE;
    }

    public void getWeatherIOWA() {
        sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_iowa_weather"));
        Dataset<Row> sqlDF = sparkSession.sql(String.format(Constant.SELECT_WEATHER_IOWA_SQL, Constant.TABLE_NAME));
        sqlDF.write().saveAsTable("bdt_iowa_weather");
        sqlDF.show();
    }

    public void getLast7DaysAvgTempByArea() {
        String sql = "SELECT zip_code, city, AVG(temperature) AS avgTemp" +
                " FROM " +
                Constant.TABLE_NAME +
                " WHERE updated_date >= DATE(NOW()) + INTERVAL -7 DAY" +
                " GROUP BY zip_code, city";

        sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_avg_weather"));
        Dataset<Row> sqlDF = sparkSession.sql(sql);
        sqlDF.write().saveAsTable("bdt_avg_weather");
        sqlDF.show();
    }

    public void getHotAreaWithTempGreaterThan83() {
        String sql = "SELECT zip_code, city, temperature as hotTemp" +
                " FROM " +
                Constant.TABLE_NAME +
                " WHERE temperature > 83" +
                " GROUP BY zip_code, city, temperature";

        sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_hot_weather"));
        Dataset<Row> sqlDF = sparkSession.sql(sql);
        sqlDF.write().saveAsTable("bdt_hot_weather");
        sqlDF.show();
    }

    public SparkSession getSession() {
        return SparkSession
                .builder()
                .appName("Spark SQL-Hive")
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
