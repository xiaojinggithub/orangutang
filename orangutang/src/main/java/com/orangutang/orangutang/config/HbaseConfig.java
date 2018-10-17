package com.orangutang.orangutang.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * hbase-client (configuration->connection)
 */
public class HbaseConfig {
    private static  final  HbaseConfig  INSTANCE=new HbaseConfig();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConfig(){
        try {
            if(configuration==null){
                configuration= HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private Connection getConnection(){
        if(connection==null||connection.isClosed()){
            try{
                connection= ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }
    public static Connection getHBaseConn(){
        return  INSTANCE.getConnection();
    }
    private static Table getTable(String tableName) throws IOException{
        return  INSTANCE.getConnection().getTable(TableName.valueOf(tableName));

    }
    private static void closeHBaseConn(){
        if(null!=connection){
            try {
                connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
