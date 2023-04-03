package extra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class AdvanceAvgTemp extends Configured implements Tool{

    public static class AdvanceAvgTempMapper extends Mapper<LongWritable, Text, StationTempKey, IntWritable> {
        private IntWritable year = new IntWritable();
        private StationTempKey stkey = new StationTempKey();

        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
            String line = value.toString();

            year.set(Integer.parseInt(line.substring(15, 19)));
            Double capturedTemp = Double.parseDouble(line.substring(87, 92));
            String stationId = line.substring(4, 10) +"-"+line.substring(10, 15);
            stkey.setTemperature(capturedTemp / 10);
            stkey.setStation(stationId);
            context.write(stkey, year);
		} 
    }

    public static class AdvanceAvgTempReducer extends Reducer<StationTempKey, IntWritable, Text, IntWritable> {
        Text stationTempkey = new Text();

        @Override
		public void reduce(StationTempKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            for (IntWritable year : values) {
                String displayedKey = key.getStation()+'\t'+ key.getTemperature();
                stationTempkey.set(displayedKey);
                context.write(stationTempkey, year);
            }
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

        int exitCode = ToolRunner.run(conf, new AdvanceAvgTemp(), args);

        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AdvanceAvgTemperature");
        job.setJarByClass(AdvanceAvgTemp.class);
    
        job.setMapperClass(AdvanceAvgTempMapper.class);
		job.setReducerClass(AdvanceAvgTempReducer.class);

		job.setMapOutputKeyClass(StationTempKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
}
