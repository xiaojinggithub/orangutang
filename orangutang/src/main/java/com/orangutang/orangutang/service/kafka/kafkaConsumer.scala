package com.orangutang.orangutang.service.kafka

import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._


/**
  * kafka  消费者
  */
object ScalaConsumerExample extends App {
  class ScalaConsumerExample() {
    val bootstrap_server: String="localhost:9092"
    val groupId: String="0"
    val topic: String="test"
    var topicList=List("test","test1")
    val props = createConsumerConfig(bootstrap_server, groupId)
     /*创建consumer和线程*/
    val consumer = new KafkaConsumer[String, String](props)
    var executor: ExecutorService = null
    /*关闭线程池和consumer*/
    def shutdown() = {
      if (consumer != null)
        consumer.close();
      if (executor != null)
        executor.shutdown();
    }

    def createConsumerConfig(brokers: String, groupId: String): Properties = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props
    }

    def run() = {
      /*这里理论上可以监听多个topic???*/
      //for(i<- 0 to topicList.size-1){
        consumer.subscribe(Collections.singletonList(this.topic))
        //consumer.subscribe(Collections.singletonList(topicList.get(i)))
      //}

      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          while (true) {
            val records = consumer.poll(1000)
            for (record <- records) {
              println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
            }
          }
        }
      })
    }
  }
}