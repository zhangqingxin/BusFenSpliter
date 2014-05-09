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
			
			//�˴�ָ��������ı���
			String table = "tb_inout_gps_103";
			Connection con = DBConnector.getConnection();
			
			//��ȡһ��ĳ����嵥
			List<Long> productList = getProductList(con, table);
			
			//��ȡվ��ָ���Ϣ
			HashMap<String, StationInfo> stationInfoMap = getStationInfoMap(con);
			
			//���������б�ȡ��ÿ����һ������ݽ��д���
			for (Long productid: productList) {
				List<InoutInfo> daylist = getBusDayRunInfoList(con, table, productid);
				processData(con, 0, daylist, stationInfoMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��ηָ��㷨
	 * @param con   ���ݿ�����
	 * @param startnum �ݹ鿪ʼʱ���б�ʼ������λ��
	 * @param daylist  һ����һ�찴ʱ�����������
	 * @param stationInfoMap  ��¼վ��ָ���Ϣ��String Ϊվ��� StationInfoΪվ��ָ���Ϣ����Ϊ�ĸ�ֵ�����п�ʼ���������п�ʼ������
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
//			//�ж�����Ǳ��������յ�һ����¼
//			if (i == 0) {
//				//����������н���վ�㣬���¼��Σ�������
//				if(info.stationnum != sInfo.upend) {
//					lastStaNum = info.stationnum;
//					insertData(info, 1);
//					processData(con, i + 1, daylist, stationInfoMap);
//					break;
//				}
//			} else {
//				//�ж����������¼����һ����¼վ�����ͬ������Ϊ��ͬһ������վ���ݣ�����
//				if (info.stationnum == lastStaNum) {
//					continue;
//				} else if (info.stationnum > lastStaNum && (info.stationnum == sInfo.downstart || info.stationnum == sInfo.upstart)) {
//					//�ж����������¼�����п�ʼվ������п�ʼվ����ͬ�����¼����
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
//					//���������¼����һ����¼վ��Ŵ��ұ�����¼վ���λ���������䣬��������²���
//					lastStaNum = info.stationnum;
//					count++;
//					lastInfo = info;
//					continue;
//				} else if (info.stationnum > lastStaNum && info.stationnum > sInfo.upstart && info.stationnum < sInfo.upend) {
//					//���������¼����һ����¼վ��Ŵ��ұ�����¼վ���λ���������䣬��������²���
//					lastStaNum = info.stationnum;
//					count++;
//					lastInfo = info;
//					continue;
//				} else if (info.stationnum < lastStaNum) {
//					//���������¼����һ����¼վ���С������еݹ�
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
