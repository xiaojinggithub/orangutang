package com.orangutang.orangutang.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * jdbc工具，非静态的方式，方便传参
 */
public final class JDBCUtils {
    private  String driver;
    private  String url;
    private  String user;
    private  String password;

    public JDBCUtils(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        /**
         * 驱动注册
         */
        try {
            Class.forName(this.driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    /**
     * 获取 Connetion
     * @return
     * @throws SQLException
     */
    public  Connection getConnection() throws SQLException{
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * 释放资源
     * @param conn
     * @param st
     * @param rs
     */
    public  void closeResource(Connection conn,Statement st,ResultSet rs) {
        closeResultSet(rs);
        closeStatement(st);
        closeConnection(conn);
    }

    /**
     * 释放连接 Connection
     * @param conn
     */
    public  void closeConnection(Connection conn) {
        if(conn !=null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        conn = null;
    }

    /**
     * 释放语句执行者 Statement
     * @param st
     */
    public  void closeStatement(Statement st) {
        if(st !=null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        st = null;
    }

    /**
     * 释放结果集 ResultSet
     * @param rs
     */
    public  void closeResultSet(ResultSet rs) {
        if(rs !=null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        rs = null;
    }
}