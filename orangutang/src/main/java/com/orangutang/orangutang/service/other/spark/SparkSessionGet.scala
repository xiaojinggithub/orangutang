package com.orangutang.orangutang.service.other.spark

import org.apache.spark.sql.SparkSession

class SparkSessionGet {

  def getSparkSession(): Unit ={
    //get SparkSession Instance
    val spark = SparkSession
      .builder().master("local[2]")
      .appName("SparkSessionGet")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    //get DataFrame
    var persontess=spark
      .read
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .json("/home/workspace/testFile/people.json")
    persontess.show()
  }
}
