package com.orangutang.orangutang.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;


/**
 * 使用JdbcTemplate来获取对hive的通用化操作
 * （其他数据库使用修改链接信息-通过jdbc能访问的数据库）
 * 通过spring的JdbcTemplate方式
 */
@Configuration
public class HiveConfig{
    @Autowired
    private Environment env;

    private DataSource dataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(env.getProperty("hive.url"));
        dataSource.setDriverClassName(env.getProperty("hive.driver-class-name"));
        dataSource.setUsername(env.getProperty("hive.username"));
        dataSource.setPassword(env.getProperty("hive.password"));
        return dataSource;
    }

    /**
     * 容器实例化bean
      * @param
     * @return
     */
    //@Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate() {
        return new JdbcTemplate(this.dataSource());
    }


       /*
        public void socket(){
        //创建Socket；连接
        final TSocket tSocket = new TSocket("192.168.56.31", 10000);
        //创建一个协议
        final TProtocol tProtcal = new TBinaryProtocol(tSocket);
        //创建Hive Client
        final HiveClient client = new HiveClient(tProtcal);
        //打开Socket
        tSocket.open();
        //执行HQL
        client.execute("desc emp");
        //处理结果
        List<String> columns = client.fetchAll();
        for(String col:columns){
            System.out.println(col);
        }
        //释放资源
        tSocket.close();
        }
        */
}
