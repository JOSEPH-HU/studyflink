package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object DemoFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MySourceFunction)

    val  data = datastream.filter(_%2==0)

    data.print().setParallelism(1)

    env.execute("DemoFilter")
  }
}
