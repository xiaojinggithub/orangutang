package com.orangutang.orangutang.service.other.hive.impl;

import com.orangutang.orangutang.service.other.hive.HiveService;
import com.orangutang.orangutang.utils.JDBCUtils;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
@Service
public class HiveServiceImpl implements HiveService {
    private Connection conn;
    private Statement statement;

    /**
     * 通过jdbc的方式访问hive
     * @param sql
     * @param url
     * @param driver
     * @return
     */
    @Override
    public ResultSet select(String sql,String url,String driver) {
        try {
            JDBCUtils  jdbcUtils=new JDBCUtils(driver,url,"hadoop","hadoop123");
            conn=jdbcUtils.getConnection();
            statement=conn.createStatement();
            ResultSet rs=statement.executeQuery(sql);
            return rs;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
