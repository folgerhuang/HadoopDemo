package com.sxt.hot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HotPartition extends Partitioner<HotDomain, Text> {

	@Override
	public int getPartition(HotDomain key, Text value, int numPartitions) {
		return (key.getYear() + "").hashCode() % numPartitions;
	}

}
