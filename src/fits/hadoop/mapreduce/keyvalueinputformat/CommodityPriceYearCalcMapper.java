package fits.hadoop.mapreduce.keyvalueinputformat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CommodityPriceYearCalcMapper extends Mapper<Text, Text, IntWritable, DoubleWritable> {

	private Logger logger = Logger.getLogger(CommodityPriceYearCalcMapper.class);

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			System.out.println("Starting CommodityPriceYearCalcMapper with key[" + key + "], value[" + value + "]");
			logger.info("Starting CommodityPriceYearCalcMapper with key[" + key + "], value[" + value + "]");

			String date = key.toString();
			Double price = Double.parseDouble(value.toString());

			logger.info("date [" + date + "], price [" + price + "]");

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date d = sdf.parse(date);
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			int year = cal.get(Calendar.YEAR);
			context.write(new IntWritable(year), new DoubleWritable(price));
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new IOException(ex.getMessage(), ex);
		}

	}
}
