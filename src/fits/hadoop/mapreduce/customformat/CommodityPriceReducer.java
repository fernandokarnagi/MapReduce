package fits.hadoop.mapreduce.customformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommodityPriceReducer extends Reducer<Text, CommodityPriceBean, Text, CommodityPriceBean> {

	@Override
	protected void reduce(Text key, Iterable<CommodityPriceBean> value,
			Reducer<Text, CommodityPriceBean, Text, CommodityPriceBean>.Context context) throws IOException,
			InterruptedException {

		double total = 0;
		int count = 0;

		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;

		for (CommodityPriceBean d : value) {
			total += d.getTotal();
			if (d.getMax() > max) {
				max = d.getMax();
			}
			if (d.getMin() < min) {
				min = d.getMin();
			}
			count++;
		}

		double average = total / count;
		context.write(new Text(key), new CommodityPriceBean(average, max, min, total, count));
	}

}
