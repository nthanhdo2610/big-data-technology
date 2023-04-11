package miu.edu.bdt.mapreduce.lab.extra3.solution;

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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class NCDCWeather extends Configured implements Tool {

    public static class NCDCWeatherMapper extends Mapper<LongWritable, Text, StationTempWritable, IntWritable> {

        private static final String STATION_ID_FORMAT = "%s-%s";

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String token = value.toString();
            int year = Integer.parseInt(token.substring(15, 19));
            float temp = Float.parseFloat(token.substring(87, 92)) / 10;
            String USAF = token.substring(4, 10);
            String WBAN = token.substring(10, 15);
            String stationId = String.format(STATION_ID_FORMAT, USAF, WBAN);
            StationTempWritable stationTempWritable = new StationTempWritable(stationId, temp);
            context.write(stationTempWritable, new IntWritable(year));

        }
    }

    public static class NCDCWeatherReducer extends Reducer<StationTempWritable, IntWritable, Text, IntWritable> {
        private final Text result = new Text();

        @Override
        public void reduce(StationTempWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            result.set(key.toString());
            for (IntWritable year : values) {
                context.write(result, year);
            }
        }
    }

    public static class CustomFileOutputFormat extends TextOutputFormat<Text, IntWritable> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter) this.getOutputCommitter(context);
            return new Path(committer.getWorkPath(), getUniqueFile(context, "part", extension).replace("part-r-00000", "StationTempRecord"));
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
        job.setMapOutputKeyClass(StationTempWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(NCDCWeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}