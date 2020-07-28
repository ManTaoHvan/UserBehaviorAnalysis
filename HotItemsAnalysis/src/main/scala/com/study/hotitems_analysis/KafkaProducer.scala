package com.study.hotitems_analysis

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("test1")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.149.111:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //注意这里是序列化，不是反序列化
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties) //KafkaProducer[k, v],k是topic ,v是数据

    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("D:\\WorkCache_IDEA\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record) // send(ProducerRecord<K, V> record)
    }
    producer.close()
  }

}
