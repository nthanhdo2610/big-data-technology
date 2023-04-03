package cs523.SparkApacheLog;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkApacheLog 
{
	// Log pattern
	private static final String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}|-) (\\d+|-)\\s?\"?([^\"]*)\"?\\s?\"?([^\"]*)?\"?$";
	// The number of log's fields that must be found
	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/MMM/yyyy:HH:mm:ss"); // [day/month/year:hour:minute:second]

	
	public static void main(String[] args) throws Exception 
	{
		// Path to access_log.txt
		final String pathToAccessLog = args[0];
		// Path to date_range.txt
		final String pathToDateRange = args[1];
		// Path to output of filtered logs (Question c)
		final String pathToOutput1 = args[2];
		// Path to output of IP address list (Question d)
		final String pathToOutput2 = args[3];
		
		
		// Remove existed “output” directory
		FileUtils.deleteDirectory(new File(pathToOutput1));
		FileUtils.deleteDirectory(new File(pathToOutput2));
		

		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));
		

		// Load access_log
		JavaRDD<String> linesAccessLog = sc.textFile(pathToAccessLog).flatMap(line -> Arrays.asList(line.split("\n")));

		// Load access_log
		List<String> linesDateRange = sc.textFile(pathToDateRange).flatMap(line -> Arrays.asList(line.split("\n"))).take(2);
		
		// Convert DateTIme
		LocalDateTime startDate = LocalDateTime.parse(linesDateRange.get(0), formatter);
		LocalDateTime endDate = LocalDateTime.parse(linesDateRange.get(1), formatter);
		
		// Filter by date range and is 401 response
		JavaRDD<String> filteredLogs = linesAccessLog.filter(l -> is401InDateRange(l, startDate, endDate));

		// Save the filtered list back out to text file
		filteredLogs.saveAsTextFile(pathToOutput1);
		
		
		// Calculate times of visited by IP address
		JavaPairRDD<String, Integer> visited = linesAccessLog.mapToPair(l -> new Tuple2<String, Integer>(getIpAddress(l), 1)).reduceByKey((x, y) -> x + y);
		
		// Filter by IP address which visited more than 20 times
		JavaPairRDD<String, Integer> filteredAddress = visited.filter(l -> l._2 > 20);

		// Save the filtered list back out to text file
		filteredAddress.saveAsTextFile(pathToOutput2);

		
		sc.close();
	}

	private static String getIpAddress(String str) 
	{
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(str);
		if (!matcher.matches()) 
		{
			System.err.println("Bad log entry (or problem with RE?):");
			System.err.println(str);
			return "Bad log entry (or problem with RE?)" + str;
		}
		
		/*
		 * System.out.println("IP Address: " + matcher.group(1));
		 * System.out.println("Date&Time: " + matcher.group(4));
		 * System.out.println("Request: " + matcher.group(5));
		 * System.out.println("Response: " + matcher.group(6));
		 * System.out.println("Bytes Sent: " + matcher.group(7)); if
		 * (!matcher.group(8).equals("-")) System.out.println("Referer: " +
		 * matcher.group(8)); System.out.println("Browser: " +
		 * matcher.group(9));
		 */

		return matcher.group(1);
	}

	private static boolean is401InDateRange(String str, LocalDateTime startDate, LocalDateTime endDate) 
	{
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(str);
		if (!matcher.matches()) 
		{
			System.err.println("Bad log entry (or problem with RE?):");
			System.err.println(str);
			return false;
		}
		
		/*
		 * System.out.println("IP Address: " + matcher.group(1));
		 * System.out.println("Date&Time: " + matcher.group(4));
		 * System.out.println("Request: " + matcher.group(5));
		 * System.out.println("Response: " + matcher.group(6));
		 * System.out.println("Bytes Sent: " + matcher.group(7)); if
		 * (!matcher.group(8).equals("-")) System.out.println("Referer: " +
		 * matcher.group(8)); System.out.println("Browser: " +
		 * matcher.group(9));
		 */
		
		// Check is in DateRange
		String temp = matcher.group(4);
		LocalDateTime date = LocalDateTime.parse(temp.substring(0, temp.length() - 6), formatter); // Don't care about zone in DateTime
		boolean isInDateRange = startDate.isBefore(date) && endDate.isAfter(date);
		
		// Check is 401
		boolean is401 = Integer.parseInt(matcher.group(6)) == 401;

		return isInDateRange && is401;
	}
}