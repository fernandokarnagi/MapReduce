package fits.hadoop.mapreduce.chainmapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fits.hadoop.mapreduce.commodityprice.CommodityPriceMapper;
import fits.hadoop.mapreduce.commodityprice.CommodityPriceReducer;
import fits.hadoop.mapreduce.commodityprice.CommodityPriceYearCalcMapper;
import fits.hadoop.mapreduce.commodityprice.CommodityPriceYearCalcReducer;

public class ChainMap extends Configured implements Tool {

	private static final String OUTPUT_PATH = "/user/fernando/chainmap/temp";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ChainMap(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Path inputPath = new Path(arg0[0]);

		Path outputDir = new Path(arg0[1]);

		Path tempDir = new Path(OUTPUT_PATH);

		// Delete output if exists
		System.out.println("Deleting temp directory - " + OUTPUT_PATH);
		FileSystem hdfs = FileSystem.get(getConf());
		if (hdfs.exists(tempDir)) {
			hdfs.delete(tempDir, true);
		}

		// Create configuration
		Configuration conf = getConf();

		// First Job

		// Create job
		Job job = new Job(conf, "CommodityPrice_AverageMonthly");
		job.setJarByClass(CommodityPriceMapper.class);

		// Setup MapReduce
		job.setMapperClass(CommodityPriceMapper.class);
		job.setReducerClass(CommodityPriceReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, tempDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;

		// Second Job

		// Create job
		job = new Job(conf, "CommodityPrice_AverageYearly");
		job.setJarByClass(CommodityPriceYearCalcMapper.class);

		// Setup MapReduce
		job.setMapperClass(CommodityPriceYearCalcMapper.class);
		job.setReducerClass(CommodityPriceYearCalcReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input
		FileInputFormat.addInputPath(job, tempDir);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job
		code = job.waitForCompletion(true) ? 0 : 1;

		return code;
	}

}
