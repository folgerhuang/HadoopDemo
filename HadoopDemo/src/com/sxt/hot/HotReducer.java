package com.sxt.hot;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HotReducer extends Reducer<HotDomain, Text, HotDomain, Text> {
	
	@Override
	protected void reduce(HotDomain key, Iterable<Text> value,
			Context context)			
			throws IOException, InterruptedException {
		for (Text text : value) {
			context.write(key, text);
		}
	}

}
