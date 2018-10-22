package com.orangutang.orangutang.controller.Spark

import com.orangutang.orangutang.service.other.spark.SparkSessionGet
import org.apache.spark.sql.SparkSession
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/spark"))
class SparkSqlController {
  @RequestMapping(value = Array("/testSparkSession"), method = Array(RequestMethod.GET))
  def testSparkSession(): Unit ={
    var t=new SparkSessionGetTest ()
    t.getSparkSessionTest()
  }
  class SparkSessionGetTest {
    def getSparkSessionTest(): Unit ={
      //get SparkSession Instance
      val spark = SparkSession
        .builder().master("local[2]")
        //name can random
        .appName("SparkSessionGetTest")
        //.config("spark.some.config.option", "some-value")
        .getOrCreate()
      //get DataFrame
      var persontess=spark
        .read
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        //read file from  local
        .json("file:///home/workspace/testFile/people.json")
      persontess.show()
    }
  }
}
