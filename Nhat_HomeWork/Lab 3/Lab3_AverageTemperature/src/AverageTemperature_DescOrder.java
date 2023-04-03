/**
 * @author Nhat Pham - 986847
 *
 * @Problem 4: Modify the above program by writing your own sorting routine so that the output file will show the latest year first. (Years should be in descending order)
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemperature_DescOrder extends Configured implements Tool
{
	public static class CombinerAvgTempMapper extends Mapper<LongWritable, Text, Text, AvgTempInYear>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			String record = value.toString();
			String year = record.substring(15, 19);
			String temp = record.substring(87, 92);			
			
			if (year != null && temp != null) {
				AvgTempInYear obj = new AvgTempInYear();
				obj.setSum(Long.valueOf(temp));
				obj.setCount(1);
				
				context.write(new Text(year), obj);
			}
		}
	}

	public static class CombinerAvgTempReducer extends Reducer<Text, AvgTempInYear, Text, DoubleWritable>
	{		
		public void reduce(Text key, Iterable<AvgTempInYear> values, Context context) throws IOException, InterruptedException 
		{
			long sum = 0;
			int count = 0;
			for (AvgTempInYear value : values) 
			{
				sum += value.getSum();
				count += value.getCount();
			}
			
			double average = sum / count;
			context.write(new Text(key.toString()), new DoubleWritable(average));
		}
	}
	
	public static class CustomKeyComparator extends WritableComparator {
	    protected CustomKeyComparator() {
	        super(Text.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable k1, WritableComparable k2) {
	        Text key1 = (Text) k1;
	        Text key2 = (Text) k2;
	        return -1*key1.compareTo(key2);
	    }
	}

	public static void main(String[] args) throws Exception
	{
		//Automatic remove existed “output” directory before job execution
		FileUtils.deleteDirectory(new File(args[1]));
		
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature_DescOrder(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf(), "AverageTemperature_Inmapper");
		job.setJarByClass(AverageTemperature_DescOrder.class);

		job.setMapperClass(CombinerAvgTempMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvgTempInYear.class);

		job.setReducerClass(CombinerAvgTempReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setSortComparatorClass(CustomKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}