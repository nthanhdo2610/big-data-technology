package miu.edu.bdt.lab.lab3.prob5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NCDCWeather extends Configured implements Tool {

    public static class NCDCWeatherMapper extends Mapper<LongWritable, Text, YearWritable, FloatIntPairWritable> {

        private Map<String, List<Float>> map = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, YearWritable, FloatIntPairWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            map = new HashMap<>();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String token = value.toString();
            String year = token.substring(15, 19);
            float temp = Float.parseFloat(token.substring(87, 92)) / 10;
            List<Float> ls = map.get(year);
            if (ls == null) {
                ls = new ArrayList<>();
            }
            ls.add(temp);
            map.put(year, ls);

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, YearWritable, FloatIntPairWritable>.Context context) throws IOException, InterruptedException {
            for (String key : map.keySet()) {
                float sum = 0.0f;
                int count = 0;
                for (Float temp : map.get(key)) {
                    sum += temp;
                    count++;
                }
                context.write(new YearWritable(Integer.parseInt(key)), new FloatIntPairWritable(sum, count));
            }
            super.cleanup(context);
        }
    }

    // Now, we need to use 2 reducers. So, create a Custom Partitioner class
    // which will send all the years less than 1930 to Reducer 1 and rest of the years to Reducer 2.
    //(Remember, partitioner will not work in local mode of your VM!)
    public static class CustomPartitioner extends HashPartitioner<YearWritable, FloatIntPairWritable> {

        @Override
        public int getPartition(YearWritable key, FloatIntPairWritable value, int numReduceTasks) {
            return key.getYear() < 1930 ? 0 : 1;
        }
    }

    public static class NCDCWeatherReducer extends Reducer<YearWritable, FloatIntPairWritable, IntWritable, FloatWritable> {
        private final FloatWritable result = new FloatWritable();
        private final IntWritable year = new IntWritable();

        @Override
        public void reduce(YearWritable key, Iterable<FloatIntPairWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatIntPairWritable pair : values) {
                sum += pair.getSum();
                count += pair.getCount();
            }
            year.set(key.getYear());
            result.set(sum / count);
            context.write(year, result);
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
        job.setMapOutputKeyClass(YearWritable.class);
        job.setMapOutputValueClass(FloatIntPairWritable.class);

        job.setReducerClass(NCDCWeatherReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}