package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object DemoUnion {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.addSource(new MySourceFunction2).flatMap(_.split("\\W+")).map((_,1))

    val stream2 = env.addSource(new MySourceFunction2).flatMap(_.split("\\W+")).map((_,1))

    val data = stream1.union(stream2).keyBy(_._1).timeWindow(Time.seconds(4)).sum(1)

    data.print().setParallelism(1)

    env.execute("DemoUnion")
  }

}
