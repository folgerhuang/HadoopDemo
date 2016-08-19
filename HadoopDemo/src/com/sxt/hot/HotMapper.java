package com.sxt.hot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jboss.netty.util.internal.StringUtil;

public class HotMapper extends Mapper<LongWritable, Text, HotDomain, Text>
{
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Calendar calendar = Calendar.getInstance();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] items = StringUtil.split(value.toString(), ',');
		if(items.length==2){
			try {
				calendar.setTime(format.parse(items[0]));
				HotDomain domain = new HotDomain();
				domain.setYear(calendar.get(1));
				domain.setHot(Integer.parseInt(items[1].substring(0,items[1].indexOf('C'))));
				context.write(domain, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
