/**
 * @author Nhat Pham - 986847
 *
 */

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StationTempRecord extends Configured implements Tool
{
	public static class StationTempRecordMapper extends Mapper<LongWritable, Text, StationTempRecordInYear, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			String record = value.toString();
			String stationId = record.substring(4, 15);
			String temp = record.substring(87, 92);
			String year = record.substring(15, 19);
			
			if (year != null && temp != null) {
				StationTempRecordInYear obj = new StationTempRecordInYear();
				obj.setStationId(Long.valueOf(stationId));
				obj.setTemp(Long.valueOf(temp));
				
				context.write(obj, new Text(year));
			}
		}
	}

	public static class StationTempRecordReducer extends Reducer<StationTempRecordInYear, Iterable<Text>, Text, Text>
	{		
		public void reduce(StationTempRecordInYear key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String str = ""; 
			for (Text value : values) 
			{
				str.concat(value.toString() + "; ");
			}
			
			context.write(new Text(key.toString()), new Text(str));
		}
	}
	
	public static class TextOutPutFileName <K,V> extends TextOutputFormat<K, V> 
	{
		@Override
		public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws java.io.IOException {
			Path path =  super.getDefaultWorkFile(context, extension);
			String str = path.toUri().getPath().replace(path.getName(), "StationTempRecord");
			
			return new Path(str);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		//Automatic remove existed “output” directory before job execution
		FileUtils.deleteDirectory(new File(args[1]));
		
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new StationTempRecord(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf(), "StationTempRecord");
		job.setJarByClass(StationTempRecord.class);

		job.setMapperClass(StationTempRecordMapper.class);
		job.setOutputKeyClass(StationTempRecordInYear.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(StationTempRecordReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutPutFileName.class);
		
		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}