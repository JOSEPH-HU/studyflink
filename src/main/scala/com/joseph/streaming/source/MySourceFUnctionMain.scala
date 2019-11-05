package com.joseph.streaming.source

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object MySourceFUnctionMain {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MySourceFunction)

    val data = datastream.map(line=>{
      println("接受数据:" + line)
      line
    })


    data.print().setParallelism(1)

    env.execute("MySourceFUnctionMain")
  }

}
