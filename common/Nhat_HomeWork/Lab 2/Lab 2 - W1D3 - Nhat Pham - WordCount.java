import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool
{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				word.set(token);
				context.write(word, one);
			}
		}
		
		
		//--------------------------------------------------------------------------------------------------------------------------------------------
		//Question C: Modify the WordCount program to output the counts of only the words “Hadoop” and “Java”. (Count Hadoop and hadoop as same word!)
		/*@Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{ 
			for (String token : value.toString().split("\\s+")) 
			{ 
				if (token.equalsIgnoreCase("Hadoop") || token.toString().equalsIgnoreCase("Java")) 
				{ 
					token = token.substring(0, 1).toUpperCase() + token.substring(1); //Do uppercase the 1st character 
					word.set(token); 
					context.write(word, one);
				}
			}
		}*/
		//--------------------------------------------------------------------------------------------------------------------------------------------
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}

		//--------------------------------------------------------------------------------------------------------------------------------------------
		//Question D: Modify the WordCount program to output the counts of only those words which appear in the document at least 25 times.
		/*@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}			
			
			if (sum >= 25) {
				result.set(sum);
				context.write(key, result);
			}
		}*/
		//--------------------------------------------------------------------------------------------------------------------------------------------

		
		//--------------------------------------------------------------------------------------------------------------------------------------------
		//Question E: Write a program to find out how many distinct words are there in the input file.
		private int wordCount;
				
		/*@Override
		protected void setup(Context context) {
			wordCount = 0;
		}

	    @Override
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	++wordCount;
	    }

	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	context.write(new Text("WordCount"), new IntWritable(wordCount));
	    }*/
		//--------------------------------------------------------------------------------------------------------------------------------------------
	}

	public static void main(String[] args) throws Exception
	{
		//Question A: automatic remove existed “output” directory before job execution
		FileUtils.deleteDirectory(new File(args[1]));
		
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new WordCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//Question B: Set up to run the Word Count program in pseudo distributed mode with 2 reducers
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
