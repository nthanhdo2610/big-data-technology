package cs523.SparkWC;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCountjdk8
{
	public static void main(String[] args) throws Exception
	{
		//Path to text_file.txt
		final String pathToText = args[0] + "text_file.txt";
		//Path to threshold.txt
		final String pathToThreshold = args[0] + "threshold.txt";
		//Path to output
		final String pathToOutput = args[1];
		
		
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));
		
		
		// Question a: Load threshold
		int threshold = Integer.valueOf(sc.textFile(pathToThreshold).first().toString());
		

		// Question b: Load our input data
		JavaRDD<String> lines = sc.textFile(pathToText);

		
		// Question c: Calculate word count
		JavaPairRDD<String, Integer> wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")))
														.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
														.reduceByKey((x, y) -> x + y);

		// Question d: Filter all words appeared less than threshold
		JavaPairRDD<String, Integer> filtered = wordCounts.filter(w -> w._2 >= threshold);
		
		
		// Question e: Count the letters
		JavaPairRDD<String, Integer> counts = filtered.flatMap(w -> Arrays.asList(w._1.split("(?!^)")))
														.mapToPair(c -> new Tuple2<String, Integer>(c, 1))
														.reduceByKey((x, y) -> x + y)
														.sortByKey();
		
		
		// Remove existed “output” directory
		FileUtils.deleteDirectory(new File(pathToOutput));
		
		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(pathToOutput);

		sc.close();
	}
}
