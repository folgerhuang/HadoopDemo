package com.sxt.hot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HotDomain implements WritableComparable<HotDomain> {
	private int year;
	private int hot;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(hot);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.hot = in.readInt();
	}

	@Override
	public int compareTo(HotDomain o) {
		int res = Integer.compare(year, o.getYear());
		if (res == 0) {
			return -Integer.compare(hot, o.getHot());
		}
		return res;
	}

	@Override
	public String toString() {
		return year + "\t" + hot;
	}

}
