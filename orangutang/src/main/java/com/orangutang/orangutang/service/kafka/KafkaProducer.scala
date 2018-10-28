package com.orangutang.orangutang.service.kafka


import java.util.Properties

import javax.jdo.annotations.Value
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * kafka生产者测试
  * 注意:maven中的kafka版本需要和安装环境中的版本相同>>目前kafka2.0 ,scala2.12
  */
object KafkaProducer {

  class  send{
    def testSend(): Unit ={
      /*基本属性参数*/
      val topic = "test"
      val brokers_list = "localhost:9092"
      val zkHost="localhost:2181"
     // @Value(s"${kafka.topic}")
      //var ss=_

      /*添加配置文件,根据官网的创建生产者的参数,只需要bootstrap.servers和topic即可*/
      val props = new Properties()
      props.put("bootstrap.servers", brokers_list)
      //props.put("client.id", "ScalaProducerExample")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      /*创建KafkaProducer,用以发送消息,这里需要为泛型指定类型*/
      val producer = new KafkaProducer[String, String](props)

      //var i=0
      for(i<-0 to 10){
        /*创建ProducerRecord*/
        var producerRecord=new ProducerRecord[String, String](topic, "key_",i.toString)
        producer.send(producerRecord)
      }

      producer.close()
    }
  }
}