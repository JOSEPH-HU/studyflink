package com.joseph.streaming.source

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object MyParallelSourceFUnctionMain {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MyParallelSourceFunction)

    val data = datastream.map(line=>{
      println("接受数据:" + line)
      line
    })

    data.print()

    env.execute("MyParallelSourceFUnctionMain")
  }

}
