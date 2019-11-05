package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object DemoWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.addSource(new MySourceFunction2)

    val data = dataStream.flatMap(_.split("\\W+")).map((_,1))
      .keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(4))).sum(1)

    data.print().setParallelism(1)

    env.execute("DemoWindow")

  }
}
