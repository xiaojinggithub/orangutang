package com.orangutang.orangutang.service.other.hive;

import java.sql.ResultSet;

/**
 * hive 查询服务层
 */
public interface HiveService {
    ResultSet  selectByJDBC(String sql,String url,String driver);
}
