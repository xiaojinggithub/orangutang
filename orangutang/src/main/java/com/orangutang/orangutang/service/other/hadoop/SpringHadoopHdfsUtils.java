package com.orangutang.orangutang.service.other.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * hadoop 文件操作工具，基于spring-hadoop
 * 基于hadoop2.7.3
 */
public class SpringHadoopHdfsUtils {

    private String localFilePath;
    private String hdfsFilePath;
    private String mkdirPath;
    /**
     * spring上下文，用以加载配置文件
     */
    private ApplicationContext ctx;
    /**
     * hadoop文件系统实体
     */
    private FileSystem  fs;


    public SpringHadoopHdfsUtils(String localFilePath, String hdfsFilePath, String mkdirPath) {
        this.localFilePath = localFilePath;
        this.hdfsFilePath = hdfsFilePath;
        this.mkdirPath=mkdirPath;
        /*load  configrtion*/
        ctx=new ClassPathXmlApplicationContext("SpringHadoopBeansConfig.xml");
        /*get fs bean from  xml config*/
        fs=(FileSystem)ctx.getBean("fs");
    }

    /**
     * 在hdfs上面创建文件夹
     * @param
     * @return
     */
    public boolean mkdir(){
        try{
            fs.mkdirs(new Path(this.mkdirPath));
        }catch (Exception e){
            e.printStackTrace();
            return  false;
        }
        return true;
    }

    /**
     * 预览文件
     * @throws Exception
     */
    public void catFile() throws  Exception{
        FSDataInputStream fsDataInputStream=fs.open(new Path(hdfsFilePath));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
        fsDataInputStream.close();
    }


}
