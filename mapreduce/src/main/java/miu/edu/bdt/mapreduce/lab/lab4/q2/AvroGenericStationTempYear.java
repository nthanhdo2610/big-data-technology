package miu.edu.bdt.mapreduce.lab.lab4.q2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class AvroGenericStationTempYear extends Configured implements Tool {

    private static Schema SCHEMA;
    private static final String DEFAULT_RESOURCES_SCHEMA_FILE_NAME = "schema/weather.avsc";

    public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
        private final NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
        private final Schema DEFAULT_RESOURCES_SCHEMA = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(DEFAULT_RESOURCES_SCHEMA_FILE_NAME));
        private final GenericRecord record = new GenericData.Record(SCHEMA != null ? SCHEMA : DEFAULT_RESOURCES_SCHEMA);
        private final AvroKey<GenericRecord> avroKey = new AvroKey<>();

        public AvroMapper() throws IOException {
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            utils.parse(value.toString());

            if (utils.isValidTemperature()) {
                record.put("stationId", utils.getStationId());
                record.put("temperature", utils.getAirTemperature());
                record.put("year", utils.getYear());
                avroKey.datum(record);
                context.write(avroKey, NullWritable.get());
            }
        }
    }

    public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {
        @Override
        protected void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//			System.out.println(key.datum());
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Avro Station-Temp-Year is running!!!!!");
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance();
        job.setJarByClass(AvroGenericStationTempYear.class);
        job.setJobName("Avro Station-Temp-Year");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String schemaFile = args[2];
        try {
            SCHEMA = new Schema.Parser().parse(new File(schemaFile));
        } catch (FileNotFoundException ignored) {
            SCHEMA = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(DEFAULT_RESOURCES_SCHEMA_FILE_NAME));
        }
        System.out.println("SCHEMA: " + SCHEMA);

        job.setMapperClass(AvroGenericStationTempYear.AvroMapper.class);
        job.setReducerClass(AvroGenericStationTempYear.AvroReducer.class);

        AvroJob.setMapOutputKeySchema(job, SCHEMA);
//		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.NULL));
        job.setMapOutputValueClass(NullWritable.class);  // This is important

        AvroJob.setOutputKeySchema(job, SCHEMA);
//		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.NULL));
        job.setOutputValueClass(NullWritable.class);  // This is important

        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[1]), true);
        int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);
        System.out.println("Avro Station-Temp-Year is done!!!!!");
        System.exit(res);
    }
}