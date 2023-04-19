package miu.edu.bdt.mapreduce.lab.lab9.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Clean output path
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);

        // Load our input data
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Calculate word count
        JavaPairRDD<String, Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .filter(w -> !w.trim().isEmpty())
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey(Integer::sum);

        int threshold = Integer.parseInt(args[2]);

        JavaPairRDD<Character, Integer> charCounts = wordCounts
                .filter(w -> w._2 > threshold)
                .flatMap(SparkWordCount::word2chars)
                .mapToPair(t -> new Tuple2<>(t._1, t._2))
                .reduceByKey(Integer::sum)
                .sortByKey();

        // Save the word count back out to a text file, causing evaluation
        charCounts.saveAsTextFile(args[1]);

        sc.close();
    }

    static List<Tuple2<Character, Integer>> word2chars(Tuple2<String, Integer> w){
        List<Tuple2<Character, Integer>> list = new ArrayList<>();
        for (char c : w._1.toCharArray()) {
            list.add(new Tuple2<>(Character.toLowerCase(c), w._2));
        }
        return list;
    }
}
