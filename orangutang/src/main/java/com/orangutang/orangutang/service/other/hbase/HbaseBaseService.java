package com.orangutang.orangutang.service.other.hbase;

import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * hbase 基础操作服务
 */
@Service
public interface HbaseBaseService {
    boolean createTable(String  tableName,String[] cfNames)throws IOException;
    Boolean putRow(String tableName,String cfName,String rowKey,String qualifier,String data);
}
