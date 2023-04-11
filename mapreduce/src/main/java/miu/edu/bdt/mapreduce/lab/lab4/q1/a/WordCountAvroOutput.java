package miu.edu.bdt.mapreduce.lab.lab4.q1.a;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountAvroOutput extends Configured implements Tool {

    public static class AvroWordCountMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<Integer>> {
        AvroKey<String> avroKey = new AvroKey<>();
        AvroValue<Integer> avroValue = new AvroValue<>(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String token : value.toString().split("\\s+")) {
                avroKey.datum(token);
                context.write(avroKey, avroValue);
            }
        }
    }

    public static class AvroWordCountReducer extends Reducer<AvroKey<String>, AvroValue<Integer>, AvroKey<String>, AvroValue<Integer>> {
        AvroValue<Integer> avroValue = new AvroValue<>();

        public void reduce(AvroKey<String> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (AvroValue<Integer> value : values) {
                sum += value.datum();
            }

            avroValue.datum(sum);
            context.write(key, avroValue);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountAvroOutput <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "WordCountAvroOutput");
        job.setJarByClass(WordCountAvroOutput.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AvroWordCountMapper.class);
        job.setReducerClass(AvroWordCountReducer.class);

        AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Schema.create(Type.INT));

        job.setInputFormatClass(TextInputFormat.class);    // This is default format; this line could've been very well omitted!
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Type.INT));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        int res = ToolRunner.run(conf, new WordCountAvroOutput(), args);
        System.exit(res);
    }
}