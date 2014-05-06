package com.zqx.busfenspilter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.zqx.busfenspilter.model.InoutInfo;
import com.zqx.busfenspilter.model.StationInfo;

public class FenSpilter {
	
	public static final int DOWNSTART = 1;
	public static final int DOWNEND = 2;
	public static final int UPSTART = 3;
	public static final int UPEND = 4;

	public static void main(String[] args) {
		try {
			String table = "tb_test_fen";
			Connection con = DBConnector.getConnection();
			
			List<Long> productList = getProductList(con, table);
			HashMap<String, StationInfo> stationInfoMap = getStationInfoMap(con);
			for (Long productid: productList) {
				List<InoutInfo> daylist = getBusDayRunInfoList(con, table, productid);
				processData(con, 0, daylist, stationInfoMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	
	public static void processData(Connection con, int startnum, List<InoutInfo> daylist, HashMap<String, StationInfo> stationInfoMap) {
		int lastStaNum = -1;
		for (int i=startnum;i<daylist.size();i++) {
			StationInfo sInfo = stationInfoMap.get(daylist.get(i).routeid);
			InoutInfo info = daylist.get(i);
			if (i == 0) {
				lastStaNum = info.stationnum;
				insertData(info, DOWNSTART);
				daylist.remove(i);
			} else {
				if (info.stationnum == lastStaNum) {
					daylist.remove(i);
					continue;
				} else if (info.stationnum > lastStaNum && info.stationnum != sInfo.downstart && info.stationnum != sInfo.downend && info.stationnum != sInfo.upstart && info.stationnum != sInfo.upend) {
					lastStaNum = info.stationnum;
					daylist.remove(i);
					continue;
				} else if (info.stationnum > lastStaNum && (info.stationnum == sInfo.downstart || info.stationnum == sInfo.downend || info.stationnum == sInfo.upstart || info.stationnum == sInfo.upend)) {
					if (info.stationnum == sInfo.downstart) {
						insertData(info, DOWNSTART);
						daylist.remove(i);
					} else if (info.stationnum == sInfo.upstart) {
						insertData(info, UPSTART);
						daylist.remove(i);
					}
					lastStaNum = info.stationnum;
				} else if (info.stationnum < lastStaNum) {
					processData(con, i, daylist, stationInfoMap);
				} else {
					insertData(info, 11111);
				}
			}
		}
	}

	public static void insertData(InoutInfo info, int up) {
		System.out.println(info.rowid + "====" + info.stationnum + "====" + info.date.toLocaleString() + "====" + up);
	}

	public static List<Long> getProductList(Connection con, String table) {
		List<Long> list = new ArrayList<Long>();
		try {
			Statement st = con.createStatement();
			String sql = "select distinct productid from " + table;
			ResultSet rs = query(st, sql);
			while(rs.next()) {
				long productid = rs.getLong("productid");
				list.add(productid);
			}
			st.close();
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public static HashMap<String, StationInfo> getStationInfoMap(Connection con) {
		HashMap<String, StationInfo> hm = new HashMap<String, StationInfo>();
		try {
			Statement st = con.createStatement();
			String sql = "select * from tb_teaminformation";
			ResultSet rs = query(st, sql);
			while(rs.next()) {
				StationInfo ss = new StationInfo();
				ss.routeid = rs.getString("xianlu");
				ss.downstart = rs.getInt("downstart");
				ss.downend = rs.getInt("downend");
				ss.upstart = rs.getInt("upstart");
				ss.upend = rs.getInt("upend");
				hm.put(ss.routeid, ss);
			}
			st.close();
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return hm;
	}

	public static List<InoutInfo> getBusDayRunInfoList(Connection con, String table, long productid) {
		List<InoutInfo> list = new ArrayList<InoutInfo>();
		try {
			Calendar c = Calendar.getInstance();
			String sql = "select * from " + table + " where productid = '" + productid + "' order by time asc";
			Statement st = con.createStatement();
			ResultSet rs = query(st, sql);
			while (rs.next()) {
				InoutInfo info = new InoutInfo();
				info.rowid = rs.getLong("ROWID");
				info.productid = rs.getLong("PRODUCTID");
				info.routeid = rs.getString("ROUTEID");
				info.stationnum = rs.getInt("STATIONSEQNUM");
				c.setTime(rs.getDate("date"));
				String[] time = rs.getTime("time").toString().split(":");
				c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), Integer.valueOf(time[0]), Integer.valueOf(time[1]), Integer.valueOf(time[2]));
				info.date = c.getTime();
				list.add(info);
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ResultSet query(Statement st, String sql) {
		long time = System.currentTimeMillis();
		try {
			ResultSet rs = st.executeQuery(sql);
			System.out.println("Query SQL :" + sql);
			System.out.println("Query Time: " + (System.currentTimeMillis() - time));
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
