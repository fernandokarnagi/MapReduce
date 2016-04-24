package fits.hadoop.mapreduce.commodityprice;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommodityPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException,
			InterruptedException {

		double val = 0;
		double count = 0;
		for (DoubleWritable d : value) {
			val += d.get();
			count++;
		}
		context.write(new Text(key), new DoubleWritable(val / count));
	}

}
