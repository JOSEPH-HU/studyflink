package com.joseph.streaming.api

import com.joseph.streaming.source.{MySourceFunction, MySourceFunction2}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object DemoFold {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.addSource(new MySourceFunction2)

    val data = dataStream.flatMap(_.split("\\W+")).map((_,1)).keyBy(_._1)
      .timeWindow(Time.seconds(4)).fold("start")((str,line)=>(str + "-" + line._1))

    data.print().setParallelism(1)
    env.execute("DemoFold")
  }

}
