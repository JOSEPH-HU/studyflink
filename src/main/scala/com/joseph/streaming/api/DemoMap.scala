package com.joseph.streaming.api

import com.joseph.streaming.source.{MySourceFunction, MySourceFunction2}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object DemoMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MySourceFunction)

    val data = datastream.map(x=>x*2).map(line=>{
      println("处理后的数据:" + line)
      line
    })

    data.print().setParallelism(1)

    env.execute("DemoMap")
  }
}
