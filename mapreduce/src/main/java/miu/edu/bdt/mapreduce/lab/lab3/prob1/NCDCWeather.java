package miu.edu.bdt.mapreduce.lab.lab3.prob1;

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

    public static class NCDCWeatherMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final Text word = new Text();

        // Write a basic MapReduce java program
        // without combiner or in-mapper combining to calculate the average temperature per year.
        // (Remember to divide the temperature by 10)
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String token = value.toString();
            String year = token.substring(15, 19);
            float temp = Integer.parseInt(token.substring(87, 92)) / 10f;
            word.set(year);
            context.write(word, new FloatWritable(temp));
        }
    }

    public static class NCDCWeatherReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0f;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
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