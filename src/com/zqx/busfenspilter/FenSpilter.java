package com.zqx.busfenspilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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
		String table = "tb_inout_gps_0327_gpsmixed";
		long time = System.currentTimeMillis();
	    fenData(table);
	    System.out.println("Total Time: " + (System.currentTimeMillis() - time) + "ms");
	}

	public static void fenData(String table) {
		try {
			Connection con = DBConnector.getConnection();
			List<Long> productList = getProductList(con, table);
			HashMap<String, StationInfo> stationInfoMap = getStationInfoMap(con);
			long time = System.currentTimeMillis();
			for (Long productid: productList) {
				List<InoutInfo> daylist = getBusDayRunInfoList(con, table, productid);
				
				con.setAutoCommit(false);
				
				//如果没有本线路班次分割标识，则跳过
				if (daylist.size() == 0 || !stationInfoMap.containsKey(daylist.get(0).routeid)) {
					continue;
				}
				
				sInoutList.clear();
				//处理数据
				processData(con, 0, daylist, stationInfoMap);

				//处理完之后没有任何班次信息，则运行如下逻辑（用于处理类似于3月26日130-102274的区间车问题）
				if (sInoutList.size() == 0) {
					if (daylist.size() > 1) {
						InoutInfo in = daylist.get(0);
						StationInfo sInInfo = stationInfoMap.get(in.routeid);
						if (sInInfo != null && in.inRangeAB(sInInfo)) {
							in.flagUpordown = 1;
							sInoutList.add(in);
						} else {
							in.flagUpordown = 3;
							sInoutList.add(in);
						}
						InoutInfo out = daylist.get(daylist.size() - 1);
						StationInfo sOutInfo = stationInfoMap.get(out.routeid);
						if (sOutInfo != null && out.inRangeAB(sOutInfo)) {
							out.flagUpordown = 2;
							sInoutList.add(out);
						} else {
							out.flagUpordown = 4;
							sInoutList.add(out);
						}
					}
				}
				
				//数据重新按照时间排序
				Collections.sort(sInoutList, new ComparatorTime());
				for (InoutInfo info: sInoutList) {
					insertData(con, info, info.flagUpordown);
				}
				
				con.commit();
				con.setAutoCommit(true);
			}
			System.out.println("Time: " + (System.currentTimeMillis() - time) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void processData(Connection con, int startnum, List<InoutInfo> daylist, HashMap<String, StationInfo> stationInfoMap) {
		
		InoutInfo lastinout = null;
		
		for (int i=startnum; i<daylist.size();i++) {
			InoutInfo inout = daylist.get(i);
			if (inout.routeid.equals("-1")) {
				continue;
			}
			StationInfo sInfo = stationInfoMap.get(inout.routeid);
			if (sInfo == null) {
				continue;
			}
//			System.out.println("downStart: " + sInfo.downstart +"   downEnd: " + sInfo.downend + "    upStart: " + sInfo.upstart + "     upEnd: " + sInfo.upend);
			if (lastinout != null && inout.stationnum == lastinout.stationnum) {
				continue;
			}
			
			//处理首尾数据
			if (i<=1 && (inout.stationnum == sInfo.upend || inout.stationnum == sInfo.downend)) {
				continue;
			} else if (i < 10 && (inout.stationnum == sInfo.downstart || inout.stationnum == sInfo.upstart)){
				if (inout.inRangeAB(sInfo)) {
					saveData(con, inout, 1);
				} else {
					saveData(con, inout, 3);
				}
				lastinout = inout;
				continue;
			} else if (inout.stationnum == sInfo.downstart && inout.stationnum == sInfo.upend || inout.stationnum == sInfo.downend || inout.stationnum == sInfo.upstart) {
				if (i > daylist.size() - 3) {
					if (inout.stationnum == sInfo.upend) {
						saveData(con, inout, 4);
						lastinout = inout;
					} else if (inout.stationnum == sInfo.downend) {
						saveData(con, inout, 2);
						lastinout = inout;
					} else {
						continue;
					}
				}
			}
			
			//处理正常情况
			if (lastinout == null || (inout.stationnum > lastinout.stationnum && ((inout.inRangeAB(sInfo) && lastinout.inRangeAB(sInfo)) || (inout.inRangeCD(sInfo) && lastinout.inRangeCD(sInfo))))) {
				//如果Xi>Xi-1且Xi<B，判断在本班次中，则跳过
				lastinout = inout;
				continue;
			}

			//如果当前数据与前一条数据对比不是同一个班次了，则从当前按数据开始，向后读取10条数据
			if ((lastinout.inRangeAB(sInfo) && inout.inRangeCD(sInfo)) || (lastinout.inRangeCD(sInfo) && inout.inRangeAB(sInfo))) {
				//读取启动数值下面序号的10个数值，然后对这10个值进行判别
				int count = i + 10;
				if (count > daylist.size()) {
					count = daylist.size();
				}
				int breakpoint = -1;
				for (int j=i+1;j<count;j++) {
					//判断，如果当前数据是在下行班次中，而且向下读取的数据存在上行班次的数据，则记录最后一个上行数据的位置
					if (inout.inRangeAB(sInfo)) {
						if (daylist.get(j).inRangeCD(sInfo)) {
							breakpoint = j;
						}
					} else {
						//判断，如果当前数据是在上行班次中，而且向下读取的数据存在下行班次的数据，则记录最后一个下行数据的位置
						if (daylist.get(j).inRangeAB(sInfo)) {
							breakpoint = j;
						}
					}
				}
				if (breakpoint > 0) {
					//如果没有混乱的数据，则直接继续处理下一条数据
					lastinout = inout;
					continue;
				} else {
					//如果存在数据混乱，则从当前点进行切分，优先判断标志位，如果没有标志位，则根据当前的数据范围进行判断
					if (inout.stationnum == sInfo.upstart) {
						saveData(con, daylist.get(i-1),2);
						saveData(con, inout, 3);
					} else if (inout.stationnum == sInfo.downstart) {
						saveData(con, inout, 1);
						saveData(con, daylist.get(i-1),4);
					} else {
						if (inout.inRangeAB(sInfo)) {
							saveData(con, inout, 1);
							saveData(con, daylist.get(i-1), 4);
						} else {
							saveData(con, daylist.get(i-1), 2);
							saveData(con, inout, 3);
						}
					}
					lastinout = inout;
					continue;
				}
			}
			
		}
	}

	public static void insertData(Connection con, InoutInfo info, int upordown) {
		String sql = "insert into tb_banci(routeid, productid, stationnum, date, time, upordown) values(?,?,?,?,?,?)";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, info.routeid);
			ps.setLong(2, info.productid);
			ps.setInt(3, info.stationnum);
			ps.setDate(4, new java.sql.Date(info.date.getTime()));
			ps.setTime(5, new java.sql.Time(info.date.getTime()));
			ps.setInt(6, upordown);
			ps.execute();
			ps.close();
			System.out.println("insert data: " + info.rowid + "====" + info.stationnum + "====" + info.productid +"========" + info.date.toLocaleString() + "====" + upordown);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	//将数据临时存储于内存中，之后重新排序或判断
	private static List<InoutInfo> sInoutList = new ArrayList<InoutInfo>();
	public static void saveData(Connection con, InoutInfo info, int up) {
		info.flagUpordown = up;
		sInoutList.add(info);
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
