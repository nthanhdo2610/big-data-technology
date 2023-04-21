package miu.edu.bdt.consumer;

import java.io.Serializable;

public class WeatherRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String date;
	private String location;
	private String minTemp;
	private String maxTemp;
	private String rainfall;
	private String evaporation;
	private String sunshine;
	private String windGustDir;
	private String windGustSpeed;
	private String windDir9am;
	private String windDir3pm;
	private String windSpeed9am;
	private String windSpeed3pm;;
	private String humidity9am;
	private String humidity3pm;
	private String pressure9am;
	private String pressure3pm;
	private String cloud9am;
	private String cloud3pm;
	private String temp9am;
	private String temp3pm;
	private String rainToday;
	private String rainTomorrow;
	
	public WeatherRecord() {
		
	}
	
	public WeatherRecord(String date, String location, String minTemp, String maxTemp, String rainfall, String evaporation, String sunshine, String windGustDir, String windGustSpeed, String windDir9am, String windDir3pm, String windSpeed9am, String windSpeed3pm, String humidity9am, String humidity3pm, String pressure9am, String pressure3pm, String cloud9am, String cloud3pm, String temp9am, String temp3pm, String rainToday, String rainTomorrow) {
		this.date = date;
		this.location = location;
		this.minTemp = minTemp;
		this.maxTemp = maxTemp;
		this.rainfall = rainfall;
		this.evaporation = evaporation;
		this.sunshine = sunshine;
		this.windGustDir = windGustDir;
		this.windGustSpeed = windGustSpeed;
		this.windDir9am = windDir9am;
		this.windDir3pm = windDir3pm;
		this.windSpeed9am = windSpeed9am;
		this.windSpeed3pm = windSpeed3pm;;
		this.humidity9am = humidity9am;
		this.humidity3pm = humidity3pm;
		this.pressure9am = pressure9am;
		this.pressure3pm = pressure3pm;
		this.cloud9am = cloud9am;
		this.cloud3pm = cloud3pm;
		this.temp9am = temp9am;
		this.temp3pm = temp3pm;
		this.rainToday = rainToday;
		this.rainTomorrow = rainTomorrow;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(String minTemp) {
		this.minTemp = minTemp;
	}

	public String getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(String maxTemp) {
		this.maxTemp = maxTemp;
	}

	public String getRainfall() {
		return rainfall;
	}

	public void setRainfall(String rainfall) {
		this.rainfall = rainfall;
	}

	public String getEvaporation() {
		return evaporation;
	}

	public void setEvaporation(String evaporation) {
		this.evaporation = evaporation;
	}

	public String getSunshine() {
		return sunshine;
	}

	public void setSunshine(String sunshine) {
		this.sunshine = sunshine;
	}

	public String getWindGustDir() {
		return windGustDir;
	}

	public void setWindGustDir(String windGustDir) {
		this.windGustDir = windGustDir;
	}

	public String getWindGustSpeed() {
		return windGustSpeed;
	}

	public void setWindGustSpeed(String windGustSpeed) {
		this.windGustSpeed = windGustSpeed;
	}

	public String getWindDir9am() {
		return windDir9am;
	}

	public void setWindDir9am(String windDir9am) {
		this.windDir9am = windDir9am;
	}

	public String getWindDir3pm() {
		return windDir3pm;
	}

	public void setWindDir3pm(String windDir3pm) {
		this.windDir3pm = windDir3pm;
	}

	public String getWindSpeed9am() {
		return windSpeed9am;
	}

	public void setWindSpeed9am(String windSpeed9am) {
		this.windSpeed9am = windSpeed9am;
	}

	public String getWindSpeed3pm() {
		return windSpeed3pm;
	}

	public void setWindSpeed3pm(String windSpeed3pm) {
		this.windSpeed3pm = windSpeed3pm;
	}

	public String getHumidity9am() {
		return humidity9am;
	}

	public void setHumidity9am(String humidity9am) {
		this.humidity9am = humidity9am;
	}

	public String getHumidity3pm() {
		return humidity3pm;
	}

	public void setHumidity3pm(String humidity3pm) {
		this.humidity3pm = humidity3pm;
	}

	public String getPressure9am() {
		return pressure9am;
	}

	public void setPressure9am(String pressure9am) {
		this.pressure9am = pressure9am;
	}

	public String getPressure3pm() {
		return pressure3pm;
	}

	public void setPressure3pm(String pressure3pm) {
		this.pressure3pm = pressure3pm;
	}

	public String getCloud9am() {
		return cloud9am;
	}

	public void setCloud9am(String cloud9am) {
		this.cloud9am = cloud9am;
	}

	public String getCloud3pm() {
		return cloud3pm;
	}

	public void setCloud3pm(String cloud3pm) {
		this.cloud3pm = cloud3pm;
	}

	public String getTemp9am() {
		return temp9am;
	}

	public void setTemp9am(String temp9am) {
		this.temp9am = temp9am;
	}

	public String getTemp3pm() {
		return temp3pm;
	}

	public void setTemp3pm(String temp3pm) {
		this.temp3pm = temp3pm;
	}

	public String getRainToday() {
		return rainToday;
	}

	public void setRainToday(String rainToday) {
		this.rainToday = rainToday;
	}

	public String getRainTomorrow() {
		return rainTomorrow;
	}

	public void setRainTomorrow(String rainTomorrow) {
		this.rainTomorrow = rainTomorrow;
	}
	
}
