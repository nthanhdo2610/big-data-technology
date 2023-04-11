package miu.edu.bdt.mapreduce.lab.lab3.prob2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class NCDCWeather extends Configured implements Tool {

    public static class NCDCWeatherMapper extends Mapper<LongWritable, Text, Text, FloatIntPairWritable> {

        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String token = value.toString();
            String year = token.substring(15, 19);
            float temp = Float.parseFloat(token.substring(87, 92)) / 10;
            word.set(year);
            context.write(word, new FloatIntPairWritable(temp, 1));
        }
    }

    // Write a MapReduce java program with combiner (no in-mapper combining)
    // to calculate the average temperature per year.
    public static class NCDCWeatherCombiner extends Reducer<Text, FloatIntPairWritable, Text, FloatIntPairWritable> {

        @Override
        public void reduce(Text key, Iterable<FloatIntPairWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatIntPairWritable pair : values) {
                sum += pair.getSum();
                count += pair.getCount();
            }
            context.write(key, new FloatIntPairWritable(sum, count));
        }
    }

    public static class NCDCWeatherReducer extends Reducer<Text, FloatIntPairWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<FloatIntPairWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatIntPairWritable pair : values) {
                sum += pair.getSum();
                count += pair.getCount();
            }
            float avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);
        int res = ToolRunner.run(conf, new NCDCWeather(), args);
        System.out.println("NCDC Weather Average finished!");
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("NCDC Weather Average running!!!");
        Job job = new Job(getConf(), "NCDCWeather");
        job.setJarByClass(NCDCWeather.class);

        job.setMapperClass(NCDCWeatherMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatIntPairWritable.class);
        job.setCombinerClass(NCDCWeatherCombiner.class);

        job.setReducerClass(NCDCWeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}