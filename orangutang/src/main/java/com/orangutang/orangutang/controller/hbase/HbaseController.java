package com.orangutang.orangutang.controller.hbase;

import com.orangutang.orangutang.service.other.hbase.HbaseBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "hbase")
public class HbaseController {
    @Autowired
    private HbaseBaseService hbaseBaseService;
    @RequestMapping(value = "createTable",method = RequestMethod.GET)
    public void createTable(){
        try{
            hbaseBaseService.createTable("file",new String[]{"prop","func"});
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @RequestMapping(value = "putData",method = RequestMethod.GET)
    public boolean putData(){
        try {
            hbaseBaseService.putRow("file","prop","1","size","20cm");
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
