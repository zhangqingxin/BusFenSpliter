package com.zqx.busfenspilter.model;

import java.sql.Time;
import java.sql.Date;

public class InoutInfo {
	public long rowid;
	public long productid;
	public String routeid;
	public int stationnum;
	public Date date;
	public Time time;
	public int error;
	
	public int flagUpordown;
	
	public boolean inRangeAB(StationInfo si) {
		if (stationnum >= si.downstart && stationnum <= si.downend) {
			return true;
		}
		return false;
	}
	
	public boolean inRangeCD(StationInfo si) {
		if (stationnum >= si.upstart && stationnum <= si.upend) {
			return true;
		}
		return false;
	}
}
