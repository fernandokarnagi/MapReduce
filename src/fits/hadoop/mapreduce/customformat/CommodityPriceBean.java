package fits.hadoop.mapreduce.customformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CommodityPriceBean implements WritableComparable {

	private double average;
	private double max;
	private double min;
	private double total;
	private int count;

	public CommodityPriceBean() {
		super();
		this.average = 0;
		this.max = 0;
		this.min = 0;
		this.total = 0;
		this.count = 0;
	}

	public CommodityPriceBean(double average, double max, double min, double total, int count) {
		super();
		this.average = average;
		this.max = max;
		this.min = min;
		this.total = total;
		this.count = count;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub

	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getTotal() {
		return total;
	}

	public void setTotal(double total) {
		this.total = total;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
