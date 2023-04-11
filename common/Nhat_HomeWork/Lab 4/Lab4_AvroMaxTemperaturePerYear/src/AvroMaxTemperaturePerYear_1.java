import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroMaxTemperaturePerYear extends Configured implements Tool
{

	private static Schema SCHEMA;

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				record.put("year", utils.getYearInt());
				record.put("temperature", utils.getAirTemperature());
				record.put("stationId", utils.getStationId());

				context.write(new AvroKey<Integer>(utils.getYearInt()), new AvroValue<GenericRecord>(record));
			}
		}
	}

	public static class AvroReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>
	{
		@Override
		protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException
		{
			GenericRecord maxRecord = null;
			
			for (AvroValue<GenericRecord> value : values) {
		        GenericRecord tempRecord = value.datum();
		        if ((maxRecord == null) || ((Float) tempRecord.get("temperature") > (Float) maxRecord.get("temperature"))) {
		        	maxRecord = tempRecord;
		        }
			}
			
			context.write(new AvroKey<GenericRecord>(maxRecord), NullWritable.get());
		}
	}
	
	/*public static class AvroKeyCustomComparator extends WritableComparator {
	    protected AvroKeyCustomComparator() {
	        super(Text.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    public int compare(AvroKey<GenericRecord> k1, AvroKey<GenericRecord> k2) {
	    	@SuppressWarnings("unchecked")
			GenericRecord key1 = ((AvroKey<GenericRecord>) k1).datum();
	    	int year1 = (int) key1.get("year");
	    	@SuppressWarnings("unchecked")
			GenericRecord key2 = ((AvroKey<GenericRecord>) k2).datum();
	    	int year2 = (int) key2.get("year");
	    	
	    	int result = (year1 == year2) ? 0 : ((year1 > year2) ? 1 : -1);	    	
	    		    	
	        return -1*result;
	    }
	}*/

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroMaxTemperaturePerYear.class);
		job.setJobName("Avro Max-Temp-Per-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);

		//Set Map output key and value classes here
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
	    AvroJob.setMapOutputValueSchema(job, SCHEMA);
	    AvroJob.setOutputKeySchema(job, SCHEMA);

		//Set the schema for the reducer output here
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(AvroKeyOutputFormat.class);

		//job.setSortComparatorClass(AvroKeyCustomComparator.class);	    
		
		//For pseudo distributed mode
		//job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroMaxTemperaturePerYear(), args);		
		System.exit(res);
	}
}