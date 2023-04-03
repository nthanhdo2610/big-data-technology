package cs523.SparkWC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(args[1]), true);

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> counts = lines
				.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.filter(w -> !w.trim().isEmpty())
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		int popularThreshold = Integer.parseInt(args[2]);

		JavaPairRDD<String, Integer> popularCounts = counts.filter(i -> i._2 > popularThreshold);

		JavaPairRDD<Character,Integer> charCounts = popularCounts.flatMap(i -> {
			List<Tuple2<Character,Integer>> list = new ArrayList<>(); 
			for (char c : i._1.toCharArray()) {
				list.add(new Tuple2<Character,Integer>(Character.toLowerCase(c),i._2));
			}
			return list.iterator();
		})
		.mapToPair(t -> new Tuple2<Character,Integer>(t._1, t._2))
		.reduceByKey((x,y) -> x+y);

		charCounts.saveAsTextFile(args[1]);

		sc.close();
	}
}
