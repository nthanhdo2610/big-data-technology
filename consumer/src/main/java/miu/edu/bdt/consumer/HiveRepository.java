package miu.edu.bdt.consumer;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveRepository implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(HiveRepository.class);
	//Date,Location,MinTemp,MaxTemp,Rainfall,Evaporation,Sunshine,WindGustDir,WindGustSpeed,WindDir9am,WindDir3pm,WindSpeed9am,WindSpeed3pm,Humidity9am,Humidity3pm,Pressure9am,Pressure3pm,Cloud9am,Cloud3pm,Temp9am,Temp3pm,RainToday,RainTomorrow
	private String CREATED_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s (date STRING, location STRING, minTemp STRING, maxTemp STRING, rainfall STRING, evaporation STRING, sunshine STRING, windGustDir STRING, windGustSpeed STRING, windDir9am STRING, windDir3pm STRING, windSpeed9am STRING, windSpeed3am STRING, humidity9am STRING, humidity3pm STRING, pressure9am STRING, pressure3pm STRING, cloud9am STRING, cloud3pm STRING, temp9am STRING, temp3pm STRING, rainToday STRING, rainTomorrow STRING) STORED AS PARQUET";
	private String INSERT_SQL = "INSERT INTO %s VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")";
		
	private static HiveRepository INSTANCE;
	
	private HiveRepository() {
		init();
	}
	
	public static HiveRepository getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new HiveRepository();			
		}
		return INSTANCE;
	}
	
	private void init() {
		try {
			Statement stmt = HiveConfig.getHiveConnection().createStatement();
			// Execute DROP TABLE Query
			stmt.execute(String.format(Constant.DROP_TABLE_SQL, Constant.TABLE_NAME_TEST));
			// Execute CREATE Query
			stmt.execute(String.format(CREATED_TABLE_SQL, Constant.TABLE_NAME_TEST));
		} catch (SQLException e) {
			logger.error("Cannot create Hive connection. " + e);
			System.exit(0);
		}
	}
	
	public void insertRecord(WeatherRecord record) {
		String sql = String.format(INSERT_SQL,
				Constant.TABLE_NAME_TEST,
				record.getDate(),
				record.getLocation(),
				record.getMinTemp(),
				record.getMaxTemp(),
				record.getRainfall(),
				record.getEvaporation(),
				record.getSunshine(),
				record.getWindGustDir(),
				record.getWindGustSpeed(),
				record.getWindDir9am(),
				record.getWindDir3pm(),
				record.getWindSpeed9am(),
				record.getWindSpeed3pm(),
				record.getHumidity9am(),
				record.getHumidity3pm(),
				record.getPressure9am(),
				record.getPressure3pm(),
				record.getCloud9am(),
				record.getCloud3pm(),
				record.getTemp9am(),
				record.getTemp9am(),
				record.getRainToday(),
				record.getRainTomorrow());
		try {
			Statement stmt = HiveConfig.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			logger.error("Cannot create Hive connection. " + e);
			System.exit(0);
		}
		
	}

//	public void batchInsert(List<WeatherRecord> records) {
//		Statement stmt = HiveConfig.createStatement();
//		try {
//			for(WeatherRecord record : records){
//				String sql = String.format(INSERT_SQL,
//						Constant.TABLE_NAME_TEST,
//						record.getDate(),
//						record.getLocation(),
//						record.getMinTemp(),
//						record.getMaxTemp(),
//						record.getRainfall(),
//						record.getEvaporation(),
//						record.getSunshine(),
//						record.getWindGustDir(),
//						record.getWindGustSpeed(),
//						record.getWindDir9am(),
//						record.getWindDir3pm(),
//						record.getWindSpeed9am(),
//						record.getWindSpeed3pm(),
//						record.getHumidity9am(),
//						record.getHumidity3pm(),
//						record.getPressure9am(),
//						record.getPressure3pm(),
//						record.getCloud9am(),
//						record.getCloud3pm(),
//						record.getTemp9am(),
//						record.getTemp9am(),
//						record.getRainToday(),
//						record.getRainTomorrow());
//				stmt.addBatch(sql);
//			}
//			stmt.executeBatch();
//		} catch (SQLException e) {
//			logger.error("Cannot create Hive connection. " + e);
//			System.exit(0);
//		}
//
//	}
	
}
