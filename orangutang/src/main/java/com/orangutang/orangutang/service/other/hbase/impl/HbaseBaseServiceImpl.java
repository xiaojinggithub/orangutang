package com.orangutang.orangutang.service.other.hbase.impl;

import com.orangutang.orangutang.config.HbaseClientConfig;
import com.orangutang.orangutang.service.other.hbase.HbaseBaseService;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
@Service
public class HbaseBaseServiceImpl implements HbaseBaseService {
    @Override
    public boolean createTable(String  tableName,String[] cfNames) throws IOException {
        try{
            Admin admin=HbaseClientConfig.getHBaseConn().getAdmin();
            if(admin.tableExists(TableName.valueOf(tableName))){
                return  false;
            }
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfNames).forEach(cf->{
                table.addFamily(new HColumnDescriptor(cf).setCompressionType(Compression.Algorithm.NONE));
            });
            admin.createTable(table);
        }catch (IOException e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入操作
      * @param tableName
     * @param cfName
     * @param rowKey
     * @param qualifier
     * @param data
     * @return
     */
    @Override
    public Boolean putRow(String tableName, String cfName, String rowKey, String qualifier, String data) {
        try {
            Table table=HbaseClientConfig.getHBaseConn().getTable(TableName.valueOf(tableName));
            Put put=new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier),Bytes.toBytes(data));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
}
