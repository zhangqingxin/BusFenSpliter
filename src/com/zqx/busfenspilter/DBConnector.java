package com.zqx.busfenspilter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DBConnector {

    private static String url = "jdbc:mysql://localhost:3306/udp?useUnicode=true&characterEncoding=UTF-8";
    final static String user = "root";
    final static String password = "1230";
    
    private static Connection sConn;
    
    public static Connection getConnection() throws SQLException {
        if (sConn != null && !sConn.isClosed()) {
            return sConn;
        } else {
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                sConn = DriverManager.getConnection(url,user, password);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sConn;
    }

//    public static void setDBName(String dbname) {
//        url += url + dbname;
//    }

}
