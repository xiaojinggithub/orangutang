package com.orangutang.orangutang.controller.spark

import java.sql.DriverManager
import java.util.Properties

import com.orangutang.orangutang.service.kafka.KafkaProducer.send
import com.orangutang.orangutang.service.kafka.ScalaConsumerExample.ScalaConsumerExample
import org.apache.spark.sql.SparkSession
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/spark"))
class SparkSqlController extends  java.io.Serializable{
  @RequestMapping(value = Array("/testSparkSession"), method = Array(RequestMethod.GET))
  def testSparkSession(): Unit ={
    var t=new SparkSessionGetTest ()
    //t.getSparkSessionTest()
    //t.sparkSqlThriftserver()
//    t.dataFrameApiOpera
    //t.RDDTOSparkDataFrame
    //var m=new getOutsideData
    //m.dealWithParquetData()
   // m.dealDataFromHive
    //m.getDataFromMysql

    var send=new send
    send.testSend()
    var test=new ScalaConsumerExample
    test.run()
  }

  /**
    * spark read local file to dataFrame
    */
  class SparkSessionGetTest{
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

    /**
      *  connect  spark  thriftserver  by jdbc
      *  代码链接spark 的thriftserver服务端(和直接链接hive对比:链接的客户端不一样)
      */
    def sparkSqlThriftserver(){
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver")
        //like beenline connect url
        var conn=DriverManager.getConnection("jdbc:hive2://192.168.1.186:10000/","hadoop","")
        var pstmt=conn.prepareStatement("select * from student")
        var rs=pstmt.executeQuery()
        while (rs.next()){
          println(rs.getString("username"))
        }

        rs.close()
        conn.close()
        pstmt.close()
      }catch {
        case e:Exception=>println(e.printStackTrace())
      }finally {

      }
    }

    /**
      * 获取数据来转换为dataframe
      */
    def dataFrameApiOpera(): Unit ={
      var spark=SparkSession
        .builder()
        .master("local[2]")
        .appName("dataFrameApiOpera")
        .getOrCreate()
      /*Loads a JSON file and returns the results as a `DataFrame`.*/
      var peopleDF=spark.read
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .json("file:///home/workspace/testFile/people.json")
      //* Prints the schema to the console in a nice tree format.
      peopleDF.printSchema
      peopleDF.show(10)
      //查询某一列,且指定别名
      peopleDF.select("firstName")
      peopleDF.select(peopleDF.col("firstName").as("name"))
      spark.stop()
    }

    /**
      * 把rdd转换为Dataframe使用Dataframe-API进行操作,或者是使用sql的方式来进行操作
      */
    def RDDTOSparkDataFrame(): Unit ={
      val spark=SparkSession.builder().appName("RDDTOSparkDataFrame").master("local[2]").getOrCreate()
      //使用rdd的api,去加载数据
      /** Read a text file from HDFS, a local file system (available on all nodes), or any
        * Hadoop-supported file system URI, and return it as an RDD of Strings.*/
      val rdd=spark.sparkContext.textFile("file:///home/workspace/testFile/rdddata.txt")
      //导入隐式转换
      import spark.implicits._
      //切割后根据bean的实例转换为dataframe,通过case class bean
      val userDF=rdd.map(_.split(",")).map(line=>user(line(0).toInt,line(1),line(2).toInt)).toDF()
      //转换为dataframe后面向dataframe操作,下面是dataframe的api进行操作
      userDF.show
      //filter
      userDF.filter(userDF.col("age")>10).show
      //创建临时表使用sparksql
      /**Creates or replaces a global temporary view using the given name. The lifetime of this
       temporary view is tied to this Spark application.*/
      //userDF.createOrReplaceGlobalTempView("user1")
      userDF.createOrReplaceTempView("user1")
      //注意:使用sql需要使用sparkSession对象
      spark.sql("select * from user1").show()

    }
  }

}
case class user(id:Int,name:String,age:Int) extends  java.io.Serializable
/*
* 处理外部数据
* */
class getOutsideData(){
  /*处理 parquet文件数据*/
  def dealWithParquetData(): Unit ={
    var spark=SparkSession.builder().master("local[2]").appName("dealWithParquetData").getOrCreate()
    //read and format parquet
    var parquetDF=spark.read.format("parquet").parquet("file:///home/workspace/testFile/resources/users.parquet")
    parquetDF.show
    //write to other data type
    parquetDF.write.format("json").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").save("/home/workspace/testFile/parquetTest.json")
    spark.stop()
  }
  /*spark操作hive表的数据  ,之前是通过spark启动thriftserver客户端,
  然后通过jdbc来进行链接,传递sql转换为spark任务,来进行查询任务的执行的
  这个执行是没有链接到启动的spark上面去执行的,而是通过本地代码的方式来进行执行的,所以没有hive的相关的配置的信息*/
  def dealDataFromHive(): Unit ={
    val spark=SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs:/user/hive/warehouse")
      .enableHiveSupport()
      .appName("dealDataFromHive")
      .getOrCreate()
    val hiveDF=spark.sql("select * from student")
    hiveDF.show()
  }

  def  getDataFromMysql(): Unit ={
    var spark=SparkSession.builder().master("local[2]").appName("getDataFromMysql").getOrCreate()
    //the way
   /* val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/hive")
      .option("dbtable", "testSchame.user")
      .option("user", "root")
      .option("password", "1234")
      .load()*/
     var properties=new  Properties
    properties.put("user", "root")
    properties.put("password", "1234")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://localhost:3306/hive", "testSchame.user", properties)
    jdbcDF2.show()
  }
}