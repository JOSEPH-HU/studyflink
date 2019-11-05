package com.joseph.streaming.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer




object Flink_kafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-1")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
      .print()

    env.execute("Flink_kafka")

  }

}
