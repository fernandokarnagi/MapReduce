package fits.hadoop.mapreduce.commodityprice;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CommodityPriceYearCalcMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	private Logger logger = Logger.getLogger(CommodityPriceYearCalcMapper.class);

	public static void main(String args[]) {
		try {
			String lineVal = "1980-01-01	842.6410504345664";

			String date = StringUtils.substring(lineVal, 0, 10);
			Double price = Double.parseDouble(StringUtils.substring(lineVal, 10, lineVal.length()));
			System.out.println("Date: " + date + ", price: " + price);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) throws IOException,
			InterruptedException {

		try {
			System.out.println("Starting CommodityPriceYearCalcMapper with key[" + key + "], value[" + value + "]");
			logger.info("Starting CommodityPriceYearCalcMapper with key[" + key + "], value[" + value + "]");

			String lineVal = value.toString();

			String date = StringUtils.substring(lineVal, 0, 10);
			Double price = Double.parseDouble(StringUtils.substring(lineVal, 10, lineVal.length()));

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
