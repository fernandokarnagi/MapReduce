package fits.hadoop.mapreduce.commodityprice;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommodityPriceYearCalcMapper extends Mapper<Text, DoubleWritable, IntWritable, DoubleWritable> {

	@Override
	protected void map(Text key, DoubleWritable value,
			Mapper<Text, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException,
			InterruptedException {

		try {
			String date = key.toString();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date d = sdf.parse(date);
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			int year = cal.get(Calendar.YEAR);
			context.write(new IntWritable(year), value);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new IOException(ex.getMessage(), ex);
		}

	}

}
