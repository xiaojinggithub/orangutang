package com.orangutang.orangutang.service.other.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.hadoop.fs.FsShell;

import java.util.Collection;

/**
 * base on  spring-boot-hadoop
 * hdfs
 */
public class SpringBootHadoopHdfsUtil implements CommandLineRunner {
    @Autowired
    private FsShell fsShell;

    @Override
    public void run(String... args) throws Exception {
        Collection<FileStatus> fileStatusCollection=fsShell.lsr("/");
        fileStatusCollection.stream().forEach(F->{
            System.err.print(F.getPath());
        });
    }
}
