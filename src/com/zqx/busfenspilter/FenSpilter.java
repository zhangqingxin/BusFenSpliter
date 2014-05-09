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
			
			//此处指定程序处理的表名
			String table = "tb_inout_gps_103";
			Connection con = DBConnector.getConnection();
			
			//获取一天的车辆清单
			List<Long> productList = getProductList(con, table);
			
			//获取站点分割信息
			HashMap<String, StationInfo> stationInfoMap = getStationInfoMap(con);
			
			//遍历车辆列表，取出每辆车一天的数据进行处理
			for (Long productid: productList) {
				List<InoutInfo> daylist = getBusDayRunInfoList(con, table, productid);
				processData(con, 0, daylist, stationInfoMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 班次分割算法
	 * @param con   数据库连接
	 * @param startnum 递归开始时，列表开始遍历的位置
	 * @param daylist  一辆车一天按时间排序的数据
	 * @param stationInfoMap  记录站点分割信息，String 为站点号 StationInfo为站点分割信息（分为四个值，下行开始结束，上行开始结束）
	 */
	public static void processData(Connection con, int startnum, List<InoutInfo> daylist, HashMap<String, StationInfo> stationInfoMap) {
		
		
		
		
		
		
//		int lastStaNum = -1;
//		InoutInfo lastInfo = null;
//		int count = 0;
//		
//		boolean isRecorded = false;
//		
//		for (int i=startnum;i<daylist.size();i++) {
//			StationInfo sInfo = stationInfoMap.get(daylist.get(i).routeid);
//			InoutInfo info = daylist.get(i);
//			//判断如果是本辆车本日第一条记录
//			if (i == 0) {
//				//如果不是上行结束站点，则记录班次，否则丢弃
//				if(info.stationnum != sInfo.upend) {
//					lastStaNum = info.stationnum;
//					insertData(info, 1);
//					processData(con, i + 1, daylist, stationInfoMap);
//					break;
//				}
//			} else {
//				//判断如果本条记录与上一条记录站点号相同，则认为是同一个到离站数据，丢弃
//				if (info.stationnum == lastStaNum) {
//					continue;
//				} else if (info.stationnum > lastStaNum && (info.stationnum == sInfo.downstart || info.stationnum == sInfo.upstart)) {
//					//判断如果本条记录与下行开始站点或上行开始站点相同，则记录数据
//					if (info.stationnum == sInfo.downstart) {
//						insertData(info, DOWNSTART);
//						processData(con, i + 1, daylist, stationInfoMap);
//						break;
//					} else if (info.stationnum == sInfo.upstart) {
//						insertData(info, UPSTART);
//						processData(con, i + 1, daylist, stationInfoMap);
//						break;
//					}
//					lastStaNum = info.stationnum;
//				} else if (info.stationnum > lastStaNum && info.stationnum > sInfo.downstart && info.stationnum < sInfo.downend) {
//					//如果本条记录比上一条记录站点号大，且本条记录站点号位于下行区间，则继续向下查找
//					lastStaNum = info.stationnum;
//					count++;
//					lastInfo = info;
//					continue;
//				} else if (info.stationnum > lastStaNum && info.stationnum > sInfo.upstart && info.stationnum < sInfo.upend) {
//					//如果本条记录比上一条记录站点号大，且本条记录站点号位于上行区间，则继续向下查找
//					lastStaNum = info.stationnum;
//					count++;
//					lastInfo = info;
//					continue;
//				} else if (info.stationnum < lastStaNum) {
//					//如果本条记录比上一条记录站点号小，则进行递归
//					processData(con, i, daylist, stationInfoMap);
//					break;
//				}
//			}
//		}
	}

	public static void insertData(InoutInfo info, int up) {
		System.out.println(info.rowid + "====" + info.stationnum + "====" + info.productid +"========" + info.date.toLocaleString() + "====" + up);
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
