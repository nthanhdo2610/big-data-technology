package miu.edu.bdt.visualizer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlHive {

	public static void main(String[] args) {
		//configure
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("Java Spark Hive Example")
				.config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
				.config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
				.config("hive.exec.scratchdir", "/tmp/the-current-user-has-permission-to-write-in")
				.config("spark.yarn.security.credentials.hive.enabled", "true")
				.config("spark.sql.hive.metastore.jars", "maven")
				.config("spark.sql.hive.metastore.version", "1.2.1")
				.config("spark.sql.catalogImplementation", "hive")
				.enableHiveSupport()
				.getOrCreate();
		
		//delete all views and tables
		spark.sql("DROP VIEW IF EXISTS bdt_tmp_weather");
		spark.sql("DROP TABLE IF EXISTS bdt_tmp_hot_weather");
		spark.sql("DROP TABLE IF EXISTS bdt_tmp_avg_weather");
		
		//create a view from a table
		spark.sql("CREATE VIEW bdt_tmp_weather AS SELECT zip_code, city, temperature, updated_date FROM bdt_weather");
		
		//create bdt_tmp_hot_weather from bdt_tmp_weather with condition
		Dataset<Row> hotArea = spark.sql("SELECT city, temperature FROM bdt_tmp_weather"
				+ " WHERE temperature > 83 GROUP BY city, temperature");
		hotArea.write().saveAsTable("bdt_tmp_hot_weather");
		
		//average temperature then create bdt_tmp_avg_weather from bdt_tmp_weather
		Dataset<Row> avgTemp = spark.sql("SELECT city, AVG(temperature) as avgTemp FROM bdt_tmp_weather" +
				" WHERE updated_date >= DATE(NOW()) + INTERVAL -1 DAY" +
				" GROUP BY city");
		avgTemp.write().saveAsTable("bdt_tmp_avg_weather");
		
		//chaining of dataset
		Dataset<Row> countCity = spark.sql("SELECT zip_code, city, COUNT(*) AS count FROM bdt_tmp_weather GROUP BY zip_code, city");
		countCity.createOrReplaceTempView("bdt_tmp_count_city");
		Dataset<Row> table = spark.sql("SELECT * FROM bdt_tmp_count_city WHERE count > 10 ");
		table.show();
		
		//query from bdt_tmp_avg_weather
		spark.sql("SELECT * FROM bdt_tmp_avg_weather ORDER BY avgTemp DESC LIMIT 10").show();
		
		//close connection
		spark.close();

	}
	
}
