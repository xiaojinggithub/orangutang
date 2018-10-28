package com.orangutang.orangutang.controller.spark

import org.apache.spark.sql.SparkSession

object SparkJob {
  def main(args: Array[String]): Unit = {
    var spark=SparkSession.builder().appName("sparkjob").master("local[*]").getOrCreate()
    //读取日志文件,使用sparkContext
    /* * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.*/
    var access=spark.sparkContext.textFile("file:///home/workspace/testFile/10000_access.log")
    //access.take(10).foreach(println)
    access.map(line=>{
      var splits=line.split(" ")
      //spark中数组使用的是()
      var ip=splits(0)
    }).take(10).foreach(println)
    spark.close()
  }
}
