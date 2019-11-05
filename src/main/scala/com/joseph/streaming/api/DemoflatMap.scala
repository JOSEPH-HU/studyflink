package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._


object DemoflatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MySourceFunction2)

    val data = datastream.flatMap(_.split(" "))

    data.print().setParallelism(1)

    env.execute("DemoflatMap")
  }
}
