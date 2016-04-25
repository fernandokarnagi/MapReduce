package fits.hadoop.mapreduce.customformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import fits.hadoop.util.PropUtil;

public class CommodityPriceMapper extends Mapper<Object, Text, Text, CommodityPriceBean> {

	private Logger logger = Logger.getLogger(CommodityPriceMapper.class);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, CommodityPriceBean>.Context context)
			throws IOException, InterruptedException {
		logger.info("Processing Mapper, parsing value [" + value.toString() + "]");
		String[] rowArr = PropUtil.csvSplit(value.toString(), ",");
		if (!rowArr[0].equalsIgnoreCase("date")) {
			logger.info("Processing date [" + rowArr[0] + "]");
			double totalSum = 0d;
			int totalRecord = 0;
			for (int i = 1; i < rowArr.length; i++) {
				if (rowArr[i] != null && rowArr[i].length() > 0) {
					totalSum = totalSum + Double.parseDouble(rowArr[i]);
					totalRecord++;
				}
			}
			double yearVal = totalSum / totalRecord;
			CommodityPriceBean bean = new CommodityPriceBean(yearVal, yearVal, yearVal, yearVal, 1);
			logger.info("Emitting bean: " + bean);
			context.write(new Text(rowArr[0]), bean);
		}
	}
}
