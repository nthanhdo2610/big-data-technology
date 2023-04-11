import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class AvgTemp extends Configured implements Tool{
    
    public static class AvgTempMapper extends Mapper<LongWritable, Text, IntWritable, StatisticsPair> {
        private IntWritable year = new IntWritable();

        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
            String line = value.toString();
            year.set(Integer.parseInt(line.substring(15, 19)));
            double capturedTemp = Double.parseDouble(line.substring(87, 92)) / 10;
            context.write(year, new StatisticsPair(capturedTemp,1));
		} 
    }

    public static class AvgTempCombiner extends Reducer<IntWritable, StatisticsPair, IntWritable, StatisticsPair>{

        @Override
		public void reduce(IntWritable key, Iterable<StatisticsPair> values, Context context) throws IOException, InterruptedException{
            Double sum = 0.0;
            Integer count = 0;
            for (StatisticsPair pair : values) {
               sum += pair.getSum();
               count += pair.getCount(); 
            }
            context.write(key, new StatisticsPair(sum,count));
        }
    }

    public static class AvgTempReducer extends Reducer<IntWritable, StatisticsPair, IntWritable, DoubleWritable> {

        @Override
		public void reduce(IntWritable key, Iterable<StatisticsPair> values, Context context) throws IOException, InterruptedException{
            double sum = 0.0;
            int count = 0;
            for (StatisticsPair pair : values) {
                sum += pair.getSum();
                count += pair.getCount();
            }
            Double avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

        int exitCode = ToolRunner.run(conf, new AvgTemp(), args);

        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgTemperature");
        job.setJarByClass(AvgTemp.class);
    
        job.setMapperClass(AvgTempMapper.class);
        job.setCombinerClass(AvgTempCombiner.class);
		job.setReducerClass(AvgTempReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(StatisticsPair.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : 1;
    }

}
