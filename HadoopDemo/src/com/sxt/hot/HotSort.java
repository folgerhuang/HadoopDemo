package com.sxt.hot;

import org.apache.hadoop.io.WritableComparator;

public class HotSort extends WritableComparator
{
	public HotSort() {
		// TODO Auto-generated constructor stub
		super(HotDomain.class,true);
	}
	@Override
	public int compare(Object a, Object b) {
		HotDomain h1 = (HotDomain)a;
		HotDomain h2 = (HotDomain)b;
		
		int res = Integer.compare(h1.getYear(), h2.getYear());
		if(res==0){
			return -(Integer.compare(h1.getHot(), h2.getHot()));
		}
		return res;
	}
}
