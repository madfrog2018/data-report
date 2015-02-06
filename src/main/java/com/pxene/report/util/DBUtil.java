package com.pxene.report.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class DBUtil {
	private static Logger log = Logger.getLogger(DBUtil.class);
	
	/**
	 * 得到数据库连接
	 * @return	conn
	 */
	public static Connection getConnection(){	
		
		
		String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://42.123.70.37:3306/wins-dsp-app?useUnicode=true&characterEncoding=utf-8";
        String user = "root";
        String password = "pxene";
              
		Connection conn = null;
		try {
			Class.forName(driver);
			
		} catch (ClassNotFoundException e1) {
			log.info("~~ mysql get driver failure ====: 加载数据库驱动失败");
			e1.printStackTrace();
			return null;
		}
		try {
			conn = DriverManager.getConnection(url, user, password);
			
		} catch (SQLException e) {
			log.info("~~ mysql getConnection exception ===:连接数据库失败");
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * app app分类关系表
	 * appId;appcategory;package
	 */
	public  void insertToAppcategory(String appId,String catcode,String apppackage){
		Connection conn =getConnection();
		PreparedStatement  pstmt = null;
        try {
			conn.setAutoCommit(false);
			String sql = "insert into dsp_t_app_category(appId,catcode,apppackage) values (?,?,?)";
			pstmt =  conn.prepareStatement(sql);
			pstmt.setString(1, appId);
			pstmt.setString(2, catcode);
			pstmt.setString(3, apppackage);
			
			int res = pstmt.executeUpdate();
			if(res == 0){
				log.info("~~ insert error row is :"+ appId+"-"+catcode+"-"+apppackage);
			}
	        conn.commit();
	        pstmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
			log.info("SQLException  ===:"+ e.getMessage());
			
		}finally{
			 try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * app使用次数表
	 * appId;time;count
	 */
	public  void insertToAppusedCount(long time,String appId,int count){
		Connection conn =getConnection();
		PreparedStatement  pstmt = null;
        try {
			conn.setAutoCommit(false);
			String sql = "insert into dsp_t_app_usedByDay_count (appId,time,count) values (?,?,?)";
			pstmt =  conn.prepareStatement(sql);
			pstmt.setString(1, appId);
			pstmt.setLong(2, time);
			pstmt.setInt(3, count);
			
			int res = pstmt.executeUpdate();
			if(res == 0){
				log.info("~~ insert error row is :"+ time+"-"+appId+"-"+count);
			}
	        conn.commit();
	        pstmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
			log.info("SQLException  ===:"+ e.getMessage());
			
		}finally{
			 try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * app使用人数表
	 * appId;time;count
	 */
	public  void insertToDeviceIdCount(long time,String appId,int count){
		Connection conn =getConnection();
		PreparedStatement  pstmt = null;
        try {
			conn.setAutoCommit(false);
			String sql = "insert into dsp_t_app_deviceId_count (appId,time,count) values (?,?,?)";
			pstmt =  conn.prepareStatement(sql);
			pstmt.setString(1, appId);
			pstmt.setLong(2, time);
			pstmt.setInt(3, count);
			
			int res = pstmt.executeUpdate();
			if(res == 0){
				log.info("~~ insert error row is :"+ time+"-"+appId+"-"+count);
			}
	        conn.commit();
	        pstmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
			log.info("SQLException  ===:"+ e.getMessage());
			
		}finally{
			 try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * app使用天数表
	 * dsp_t_app_usedDays_count
	 */
	public  void insertToUsedDaysCount(long time,String appId,int count){
		Connection conn =getConnection();
		PreparedStatement  pstmt = null;
        try {
			conn.setAutoCommit(false);
			String sql = "insert into dsp_t_app_usedDays_count (appId,time,count) values (?,?,?)";
			pstmt =  conn.prepareStatement(sql);
			pstmt.setString(1, appId);
			pstmt.setLong(2, time);
			pstmt.setInt(3, count);
			
			int res = pstmt.executeUpdate();
			if(res == 0){
				log.info("~~ insert error row is :"+ time+"-"+appId+"-"+count);
			}
	        conn.commit();
	        pstmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
			log.info("SQLException  ===:"+ e.getMessage());
			
		}finally{
			 try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 日使用人数
	 * dsp_t_app_deviceIdByDay_count
	 */
	public  void insertToDeviceIdByDayCount(long time,String appId,int count){
		Connection conn =getConnection();
		PreparedStatement  pstmt = null;
        try {
			conn.setAutoCommit(false);
			String sql = "insert into dsp_t_app_deviceIdByDay_count (appId,time,count) values (?,?,?)";
			pstmt =  conn.prepareStatement(sql);
			pstmt.setString(1, appId);
			pstmt.setLong(2, time);
			pstmt.setInt(3, count);
			
			int res = pstmt.executeUpdate();
			if(res == 0){
				log.info("~~ insert error row is :"+ time+"-"+appId+"-"+count);
			}
	        conn.commit();
	        pstmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
			log.info("SQLException  ===:"+ e.getMessage());
			
		}finally{
			 try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
}
