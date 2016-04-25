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
		CommodityPriceBean rhs = (CommodityPriceBean) o;
		if (rhs.getTotal() > this.getTotal()) {
			return -1;
		} else if (rhs.getTotal() < this.getTotal()) {
			return -1;
		} else {
			return 0;
		}
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		average = input.readDouble();
		max = input.readDouble();
		min = input.readDouble();
		total = input.readDouble();
		count = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeDouble(average);
		output.writeDouble(max);
		output.writeDouble(min);
		output.writeDouble(total);
		output.writeInt(count);
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(average);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + count;
		temp = Double.doubleToLongBits(max);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(min);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(total);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CommodityPriceBean other = (CommodityPriceBean) obj;
		if (Double.doubleToLongBits(average) != Double.doubleToLongBits(other.average))
			return false;
		if (count != other.count)
			return false;
		if (Double.doubleToLongBits(max) != Double.doubleToLongBits(other.max))
			return false;
		if (Double.doubleToLongBits(min) != Double.doubleToLongBits(other.min))
			return false;
		if (Double.doubleToLongBits(total) != Double.doubleToLongBits(other.total))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return average + "|" + max + "|" + min + "|" + total + "|" + count;
	}

}
