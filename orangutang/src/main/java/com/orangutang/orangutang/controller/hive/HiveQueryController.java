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
        String sql="select * from student";
        String url="jdbc:hive://localhost:10000/default";
        String driver="org.apache.hadoop.hive.jdbc.HiveDriver";
        ResultSet rs=hiveService.select(sql,url,driver);
        while (rs.next()){
            System.err.print("name>>"+(String)rs.getObject(1));
        }
    }
}
