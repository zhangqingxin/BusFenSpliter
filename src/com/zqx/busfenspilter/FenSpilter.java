package com.zqx.busfenspilter;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
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
			String table = "tb_fen_test";
			Connection con = DBConnector.getConnection();
			List<Long> productList = getProductList(con, table);
			HashMap<String, StationInfo> stationInfoMap = getStationInfoMap(con);
			for (Long productid: productList) {
				List<InoutInfo> daylist = getBusDayRunInfoList(con, table, productid);
				con.setAutoCommit(false);
				processData(con, 0, daylist, stationInfoMap);
				con.commit();
				con.setAutoCommit(true);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void processData(Connection con, int startnum, List<InoutInfo> daylist, HashMap<String, StationInfo> stationInfoMap) {
		
		InoutInfo lastinout = null;
		
		for (int i=startnum; i<daylist.size();i++) {
			InoutInfo inout = daylist.get(i);
			StationInfo sInfo = stationInfoMap.get(inout.routeid);
//			System.out.println("downStart: " + sInfo.downstart +"   downEnd: " + sInfo.downend + "    upStart: " + sInfo.upstart + "     upEnd: " + sInfo.upend);
			if (lastinout != null && inout.stationnum == lastinout.stationnum) {
				continue;
			}
			//判断是否是上行结束的标志，如果是则跳过
			if (i==0 && inout.stationnum == sInfo.upend) {
				continue;
			} else if (inout.stationnum == sInfo.downstart){
				if (i < 10) {
					insertData(con, inout, 1);
					lastinout = inout;
					continue;
				}
			} else if (inout.stationnum == sInfo.upend) {
				if (i == daylist.size() - 1) {
					insertData(con, inout, 4);
				}
			}
			
			if (lastinout == null || (inout.stationnum > lastinout.stationnum && ((inout.inRangeAB(sInfo) && lastinout.inRangeAB(sInfo)) || (inout.inRangeCD(sInfo) && lastinout.inRangeCD(sInfo))))) {
				//如果Xi>Xi-1且Xi<B，判断在本班次中，则跳过
				lastinout = inout;
				continue;
			}

			//若B没有丢失，则肯定会遇到B，若B丢失，C存在的话，则会碰到C，若C也丢失，则会碰到C下面的值
			if ((lastinout.inRangeAB(sInfo) && inout.inRangeCD(sInfo)) || (lastinout.inRangeCD(sInfo) && inout.inRangeAB(sInfo))) {
				//读取启动数值下面序号的10个数值，放于一个数据结构，如数组，然后对这10个值进行判别
				int count = i + 10;
				if (count > daylist.size()) {
					count = daylist.size();
				}
				int breakpoint = -1;
				for (int j=i+1;j<=count;j++) {
					//FIXME:这里需要再看看，是不是这样写遇到班次不停反复是否会出问题
					if (inout.inRangeAB(sInfo)) {
						if (daylist.get(j).inRangeCD(sInfo)) {
							breakpoint = j;
						}
					} else {
						if (daylist.get(j).inRangeAB(sInfo)) {
							breakpoint = j;
						}
					}
				}
				if (breakpoint > 0) {
					//FIXME: 此处也需要考虑一下
//					if (inout.inRangeAB(sInfo)) {
//						insertData(daylist.get(i - 1), 2);
//						insertData(daylist.get(breakpoint + 1), 3);
//					} else {
//						insertData(daylist.get(i - 1), 4);
//						insertData(daylist.get(breakpoint + 1), 1);
//					}
					lastinout = inout;
					continue;
				} else {
					if (inout.stationnum == sInfo.upstart) {
						insertData(con, daylist.get(i-1),2);
						insertData(con, inout, 3);
					} else if (inout.stationnum == sInfo.downstart) {
						insertData(con, inout, 1);
						insertData(con, daylist.get(i-1),4);
					} else {
						if (inout.inRangeAB(sInfo)) {
							insertData(con, inout, 1);
							insertData(con, daylist.get(i-1), 4);
						} else {
							insertData(con, daylist.get(i-1), 2);
							insertData(con, inout, 3);
						}
					}
					lastinout = inout;
					continue;
				}
			}
			
		}
	}

	public static void insertData(Connection con, InoutInfo info, int up) {
		String sql = "insert into tb_banci(routeid, productid, stationnum, date, time, upordown) values(?,?,?,?,?,?)";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, info.routeid);
			ps.setLong(2, info.productid);
			ps.setInt(3, info.stationnum);
			ps.setDate(4, new java.sql.Date(info.date.getTime()));
			ps.setTime(5, new java.sql.Time(info.date.getTime()));
			ps.setInt(6, up);
			ps.execute();
			ps.close();
			System.out.println("insert data: " + info.rowid + "====" + info.stationnum + "====" + info.productid +"========" + info.date.toLocaleString() + "====" + up);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
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
