package com.orangutang.orangutang.controller.hive;

import com.orangutang.orangutang.service.other.hive.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("hive")
public class HiveQueryController {
    @Autowired
    private HiveService hiveService;

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * jDBC  hive-jdbc.jar
     */
    @RequestMapping(value = "test",method = RequestMethod.GET)
    public void testQuery(String sql) throws Exception{
        //String sql="show databases";
        /*注意hive的版本*/
        String url="jdbc:hive2://localhost:10000/";
        String driver="org.apache.hive.jdbc.HiveDriver";
        ResultSet rs=hiveService.selectByJDBC(sql,url,driver);
        if (null!=rs){
            while (rs.next()){
                System.err.println("name>>"+(String)rs.getObject(2));
            }
        }
    }

    /**
     * 通过向spring注册的jdbcTemplate的方式
     */
    @RequestMapping(value = "template")
    public  void testJDBCTemplate(){
        String sql="select * from  student";
        List<Map<String, Object>> rows =hiveJdbcTemplate.queryForList(sql);
        Iterator<Map<String, Object>> it = rows.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            System.out.println(String.format("%s\t%s", row.get("key"), row.get("value")));
        }
    }
}
