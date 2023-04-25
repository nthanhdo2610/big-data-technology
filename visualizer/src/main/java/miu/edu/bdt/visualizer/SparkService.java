package miu.edu.bdt.visualizer;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkService {

    private static SparkSession sparkSession = null;
    private static SparkService INSTANCE = null;

    private SparkService() {
    	final SparkConf sparkConf = new SparkConf();
    	
    	sparkConf.setMaster("local");
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
    
    public List<Weather> getWeatherIOWA() {
		sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_iowa_weather"));
		Dataset<Row> sqlDF = sparkSession.sql(String.format(Constant.SELECT_WEATHER_IOWA_SQL, Constant.TABLE_NAME));
		sqlDF.write().saveAsTable("bdt_iowa_weather");
		sqlDF.show();
		
//		List<Weather> list = sqlDF.as(Encoders.bean(Weather.class)).collectAsList();		
		return null;
	}
	
	public List<AvgTemperature> getLast7DaysAvgTempByArea() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT zip_code, city, AVG(temperature) AS avgTemp")
			.append(" FROM ")
			.append(Constant.TABLE_NAME)
			.append(" WHERE updated_date >= DATE(NOW()) + INTERVAL -7 DAY")
			.append(" GROUP BY zip_code, city");
		
		sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_avg_weather"));
		Dataset<Row> sqlDF = sparkSession.sql(sql.toString());
		sqlDF.write().saveAsTable("bdt_avg_weather");
		sqlDF.show();

//		List<AvgTemperature> list = sqlDF.as(Encoders.bean(AvgTemperature.class)).collectAsList();
		return null;
	}
	
	public List<AvgTemperature> getHotAreaWithTempGreaterThan83() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT zip_code, city, temperature as hotTemp")
	        .append(" FROM ")
	        .append(Constant.TABLE_NAME)
	        .append(" WHERE temperature > 83")
	        .append(" GROUP BY zip_code, city, temperature");
	        
		sparkSession.sql(String.format(Constant.DROP_TABLE_SQL, "bdt_hot_weather"));
		Dataset<Row> sqlDF = sparkSession.sql(sql.toString());
		sqlDF.write().saveAsTable("bdt_hot_weather");
		sqlDF.show();
		
//		List<AvgTemperature> list = sqlDF.as(Encoders.bean(AvgTemperature.class)).collectAsList();
		return null;
	}
}
