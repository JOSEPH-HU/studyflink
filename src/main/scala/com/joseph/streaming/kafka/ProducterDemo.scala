package com.joseph.streaming.kafka

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


object ProducterDemo {

  val brokers = "localhost:9092"
  val topic = "test";
  val events = 100

  def main(args: Array[String]): Unit = {

    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime();
      val ip = "192.168.2." + rnd.nextInt(255);
      val msg = runtime + "," + nEvents + ",www.example.com," + ip;
      val data = new ProducerRecord[String, String](topic, ip, msg);
      producer.send(data);
    }
    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t));
    producer.close();

  }

}
