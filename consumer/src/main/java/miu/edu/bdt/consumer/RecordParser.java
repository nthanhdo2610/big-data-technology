package miu.edu.bdt.consumer;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class RecordParser {

private static final Logger logger = LoggerFactory.getLogger(RecordParser.class);
	
	private static final CSVParser PARSER = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();

	
	public static WeatherRecord parse(Tuple2<String, String> tuple2) {
		return parse(tuple2._2());
	}
	
	public static WeatherRecord parse(String line) {
		try {
			String[] fields = PARSER.parseLine(line);
			
			if (fields.length < 23) {
				return null;				
			}
			
			String date = fields[0];
			String location = fields[1];
			String minTemp = fields[2];
			String maxTemp = fields[3];
			String rainfall = fields[4];
			String evaporation = fields[5];
			String sunshine = fields[6];
			String windGustDir = fields[7];
			String windGustSpeed = fields[8];
			String windDir9am = fields[9];
			String windDir3pm = fields[10];
			String windSpeed9am = fields[11];
			String windSpeed3pm = fields[12];
			String humidity9am = fields[13];
			String humidity3pm = fields[14];
			String pressure9am = fields[15];
			String pressure3pm = fields[16];
			String cloud9am = fields[17];
			String cloud3pm = fields[18];
			String temp9am = fields[19];
			String temp3pm = fields[20];
			String rainToday = fields[21];
			String rainTomorrow = fields[22];

			return new WeatherRecord(date, location, minTemp, maxTemp, rainfall, evaporation, sunshine,
					windGustDir, windGustSpeed, windDir9am, windDir3pm, windSpeed9am, windSpeed3pm,
					humidity9am, humidity3pm, pressure9am, pressure3pm, cloud9am, cloud3pm, temp9am,
					temp3pm, rainToday, rainTomorrow);
		} catch (Exception e) {
			logger.warn("Cannot parse record. [" + line + "] " + e);
			return null;
		}
	}
	
	public static WeatherRecord parse(Row row) {
		String date = row.getString(0);
		String location = row.getString(1);
		String minTemp = row.getString(2);
		String maxTemp = row.getString(3);
		String rainfall = row.getString(4);
		String evaporation = row.getString(5);
		String sunshine = row.getString(6);
		String windGustDir = row.getString(7);
		String windGustSpeed = row.getString(8);
		String windDir9am = row.getString(9);
		String windDir3pm = row.getString(10);
		String windSpeed9am = row.getString(11);
		String windSpeed3pm = row.getString(12);
		String humidity9am = row.getString(13);
		String humidity3pm = row.getString(14);
		String pressure9am = row.getString(15);
		String pressure3pm = row.getString(16);
		String cloud9am = row.getString(17);
		String cloud3pm = row.getString(18);
		String temp9am = row.getString(19);
		String temp3pm = row.getString(20);
		String rainToday = row.getString(21);
		String rainTomorrow = row.getString(22);
		
		return new WeatherRecord(date, location, minTemp, maxTemp, rainfall, evaporation, sunshine,
				windGustDir, windGustSpeed, windDir9am, windDir3pm, windSpeed9am, windSpeed3pm,
				humidity9am, humidity3pm, pressure9am, pressure3pm, cloud9am, cloud3pm, temp9am,
				temp3pm, rainToday, rainTomorrow);
	}
	
}
