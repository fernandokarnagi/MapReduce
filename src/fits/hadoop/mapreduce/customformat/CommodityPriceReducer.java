package fits.hadoop.mapreduce.customformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CommodityPriceReducer extends Reducer<Text, CommodityPriceBean, Text, CommodityPriceBean> {

	private Logger logger = Logger.getLogger(CommodityPriceReducer.class);

	@Override
	protected void reduce(Text key, Iterable<CommodityPriceBean> value,
			Reducer<Text, CommodityPriceBean, Text, CommodityPriceBean>.Context context) throws IOException,
			InterruptedException {

		double total = 0;
		int count = 0;

		logger.info("Processing reducer key [" + key + "]");

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
		CommodityPriceBean bean = new CommodityPriceBean(average, max, min, total, count);
		logger.info("Emitting bean [" + bean + "]");
		context.write(new Text(key), bean);
	}

}
