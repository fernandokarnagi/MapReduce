package fits.hadoop.mapreduce.customformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommodityPrice extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CommodityPrice(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// Path inputPath = new
		// Path("/Users/fernando/Work/Apps/Hadoop/commodityprice/");
		// Path outputDir = new
		// Path("/Users/fernando/Work/Apps/Hadoop/commodityprice/output");

		Path inputPath = new Path(arg0[0]);
		Path outputDir = new Path(arg0[1]);

		// Create configuration
		Configuration conf = getConf();

		// Create job
		Job job = new Job(conf, "CommodityPrice");
		job.setJarByClass(CommodityPriceMapper.class);

		// Setup MapReduce
		job.setMapperClass(CommodityPriceMapper.class);
		job.setReducerClass(CommodityPriceReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CommodityPriceBean.class);

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
		return code;
	}

}
