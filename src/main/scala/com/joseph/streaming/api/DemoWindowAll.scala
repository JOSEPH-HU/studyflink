package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object DemoWindowAll {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val steam = env.addSource(new MySourceFunction2)

    val data = steam.flatMap(_.split("\\W+")).map((_,1)).keyBy(0)
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(4))).sum(1)

    data.print().setParallelism(1)

    env.execute("DemoWindowAll")
  }

}
