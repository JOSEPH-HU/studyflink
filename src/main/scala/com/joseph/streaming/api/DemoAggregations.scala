package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object DemoAggregations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream = env.addSource(new MySourceFunction2)


    val counts = datastream.flatMap { _.split("\\W+") }
      .map { line=>(line,1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print().setParallelism(1)

    env.execute("DemoKeyBy")
  }

}
