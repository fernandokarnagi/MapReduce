package fits.hadoop.mapreduce.commodityprice;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fits.hadoop.util.PropUtil;

public class CommodityPriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] rowArr = PropUtil.csvSplit(value.toString(), ",");
		if (!rowArr[0].equalsIgnoreCase("date")) {
			System.out.println("Processing date [" + rowArr[0] + "]");
			double totalSum = 0d;
			int totalRecord = 0;
			for (int i = 1; i < rowArr.length; i++) {
				if (rowArr[i] != null && rowArr[i].length() > 0) {
					totalSum = totalSum + Double.parseDouble(rowArr[i]);
					totalRecord++;
				}
			}
			context.write(new Text(rowArr[0]), new DoubleWritable(totalSum / totalRecord));
		}
	}

	// private final IntWritable ONE = new IntWritable(1);
	// private Text word = new Text();

}
