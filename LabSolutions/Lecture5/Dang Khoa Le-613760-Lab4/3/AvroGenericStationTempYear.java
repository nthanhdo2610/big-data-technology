package khoa;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericStationTempYear extends Configured implements Tool
{

	private static Schema SCHEMA;
	private static final String YEAR_FIELD="year";
	private static final String TEMPERATURE_FIELD="temperature";

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, AvroValue<Text>>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);
		AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);
		Text stationId = new Text();
		AvroValue<Text> avroValue = new AvroValue<>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				stationId.set(utils.getStationId());
				record.put(TEMPERATURE_FIELD, utils.getAirTemperature());
				
				// __________ populate the GenericRecord with year here 
				record.put(YEAR_FIELD, utils.getYear());	
				
				//___________populate the AvroKey with record object here
				avroKey.datum(record);	
				avroValue.datum(stationId);
				
				context.write(avroKey, avroValue);
			}
		}
	}

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, AvroValue<Text>, AvroKey<GenericRecord>, AvroValue<Text>>
	{
		String offsetYear = "";

		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<Text>> values,
				Context context) throws IOException, InterruptedException
		{
			String currentYear = key.datum().get(YEAR_FIELD).toString();
			if(!offsetYear.equals(currentYear)){
				offsetYear = currentYear;
				context.write(key, values.iterator().next());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
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

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);
		
		
		AvroJob.setMapOutputKeySchema(job, SCHEMA);  
		AvroJob.setMapOutputValueSchema(job, Schema.create(Type.STRING));
		
		// Need to set output key schema as follows:
		AvroJob.setOutputKeySchema(job, SCHEMA);
		AvroJob.setOutputValueSchema(job, Schema.create(Type.STRING));

		//Should the following line be commented or uncommented? Take the correct action.
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(args[1]), true);
		int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);		
		System.exit(res);
	}
}
