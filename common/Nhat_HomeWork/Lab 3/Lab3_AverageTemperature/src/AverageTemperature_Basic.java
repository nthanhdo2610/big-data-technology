/**
 * @author Nhat Pham - 986847
 *
 * @Problem 1: Write a basic MapReduce java program without combiner or in-mapper combining to calculate the average temperature per year.
 */

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageTemperature_Basic extends Configured implements Tool
{
	public static class BasicAvgTempMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			String record = value.toString();
			String year = record.substring(15, 19);
			String temp = record.substring(87, 92);			
			
			if (year != null && temp != null) {
				context.write(new Text(year), new LongWritable(Long.valueOf(temp)));
			}
		}
	}

	public static class BasicAvgTempReducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
	{		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
		{
			long sum = 0;
			int count = 0;
			for (LongWritable value : values) 
			{
				sum += value.get();
				count++;
			}
			
			double average = sum / count;
			context.write(new Text(key.toString()), new DoubleWritable(average));
		}
	}

	public static void main(String[] args) throws Exception
	{
		//Automatic remove existed “output” directory before job execution
		FileUtils.deleteDirectory(new File(args[1]));
		
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature_Basic(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(AverageTemperature_Basic.class);

		job.setMapperClass(BasicAvgTempMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(BasicAvgTempReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}