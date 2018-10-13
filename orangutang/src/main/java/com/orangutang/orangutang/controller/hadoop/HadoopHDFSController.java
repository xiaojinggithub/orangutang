package com.orangutang.orangutang.controller.hadoop;

import com.orangutang.orangutang.service.other.hadoop.SpringBootHadoopHdfsUtil;
import com.orangutang.orangutang.service.other.hadoop.SpringHadoopHdfsUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("hadoop")
public class HadoopHDFSController {
    /*hdfs mkdir*/
    @RequestMapping(value = "mkdir",method = RequestMethod.GET)
    public void mkdir( String mkdirPath) throws Exception{
        SpringHadoopHdfsUtils  springHadoopHdfsUtils=new SpringHadoopHdfsUtils(null,null,mkdirPath);
        springHadoopHdfsUtils.mkdir();
    }
    @RequestMapping(value = "catFile",method = RequestMethod.GET)
    public void catFile(String hdfsFilePath) throws Exception{
        SpringHadoopHdfsUtils  springHadoopHdfsUtils=new SpringHadoopHdfsUtils(null,hdfsFilePath,null);
        springHadoopHdfsUtils.catFile();
    }
    @RequestMapping(value = "springbootHadoopTest",method = RequestMethod.GET)
    public void testHadoopForSpringBoot(){
        //SpringApplication.run(SpringBootHadoopHdfsUtil.class,null);
        SpringBootHadoopHdfsUtil springBootHadoopHdfsUtil=new SpringBootHadoopHdfsUtil();
    }
}
