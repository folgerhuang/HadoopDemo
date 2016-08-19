package com.sxt.hot;

import org.apache.hadoop.io.WritableComparator;

public class HotGroup extends WritableComparator{
	public HotGroup() {
		super(HotDomain.class,true);
	}
	
	@Override
	public int compare(Object a, Object b) {
		return Integer.compare(((HotDomain)a).getYear(), ((HotDomain)b).getYear());
	}
}
