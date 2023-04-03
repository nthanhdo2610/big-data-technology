import java.io.IOException;
import java.util.HashMap;

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
        private HashMap<Integer, StatisticsPair> map = new HashMap<>();

        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
            String line = value.toString();
            Integer year = Integer.parseInt(line.substring(15, 19));
            double capturedTemp = Double.parseDouble(line.substring(87, 92)) / 10;
            if(map.containsKey(year)){
                StatisticsPair pair = map.get(year);
                map.put(year, new StatisticsPair(pair.getSum() + capturedTemp, pair.getCount() + 1));
            }else{
                map.put(year, new StatisticsPair(capturedTemp, 1));
            }
		} 

        @Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Integer year : map.keySet()) {
                context.write(new IntWritable(year), map.get(year));
            }
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
