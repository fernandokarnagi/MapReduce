package fits.hadoop.mapreduce.keyvalueinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class YearAggregrator extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new YearAggregrator(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Path inputPath = new Path(arg0[0]);

		Path outputDir = new Path(arg0[1]);

		// Create configuration
		Configuration conf = getConf();

		// Create job
		Job job = new Job(conf, "CommodityPrice_AverageYearly");
		job.setJarByClass(CommodityPriceYearCalcMapper.class);

		// Setup MapReduce
		job.setMapperClass(CommodityPriceYearCalcMapper.class);
		job.setReducerClass(CommodityPriceYearCalcReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input	
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;

		return code;
	}

}
