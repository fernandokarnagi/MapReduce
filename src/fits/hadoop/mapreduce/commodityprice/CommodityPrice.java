package fits.hadoop.mapreduce.commodityprice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CommodityPrice {

	public static void main(String[] args) throws Exception {
		Path inputPath = new Path("/Users/fernando/Work/Apps/Hadoop/wordcount/FL_insurance_sample.csv");
		Path outputDir = new Path("/Users/fernando/Work/Apps/Hadoop/wordcount/output");

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(CommodityPriceMapper.class);

		// Setup MapReduce
		job.setMapperClass(CommodityPriceMapper.class);
		job.setReducerClass(CommodityPriceReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
