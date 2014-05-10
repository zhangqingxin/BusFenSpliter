package com.zqx.busfenspilter;

import java.util.Comparator;

import com.zqx.busfenspilter.model.InoutInfo;

public class ComparatorTime implements Comparator<InoutInfo> {
	@Override
	public int compare(InoutInfo o1, InoutInfo o2) {
		if (o2.date.getTime() <= o1.date.getTime()) {
			return 1;
		}
		return 0;
	}

}
