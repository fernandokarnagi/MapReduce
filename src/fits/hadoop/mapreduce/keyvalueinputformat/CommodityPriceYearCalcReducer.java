package fits.hadoop.mapreduce.keyvalueinputformat;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CommodityPriceYearCalcReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	private Logger logger = Logger.getLogger(CommodityPriceYearCalcReducer.class);

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> value,
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException,
			InterruptedException {

		int year = key.get();
		System.out.println("year: " + year);
		logger.info("Starting CommodityPriceYearCalcReducer, year: " + year);

		double total = 0d;
		int count = 0;

		for (DoubleWritable d : value) {
			count++;
			total += d.get();

		}
		logger.info("total: " + total + ", count: " + count);
		context.write(new IntWritable(year), new DoubleWritable(total / count));
	}

}
