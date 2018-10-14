package com.orangutang.orangutang.controller.hive;

import com.orangutang.orangutang.service.other.hive.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;

@RestController
@RequestMapping("hive")
public class HiveQueryController {
    @Autowired
    private HiveService hiveService;

    /**
     * jDBC  hive-jdbc.jar
     */
    @RequestMapping(value = "test",method = RequestMethod.GET)
    public void testQuery() throws Exception{
        String sql="show databases";
        /*注意hive的版本*/
        String url="jdbc:hive2://192.168.31.32:10000/";
        String driver="org.apache.hive.jdbc.HiveDriver";
        ResultSet rs=hiveService.select(sql,url,driver);
        if (null!=rs){
            while (rs.next()){
                System.err.print("name>>"+(String)rs.getObject(1));
            }
        }
    }
}
