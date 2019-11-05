package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object DemoReduce {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.addSource(new MySourceFunction2)

    val data = dataStream.flatMap(_.split("\\W+")).map((_,1)).keyBy(_._1)
      .timeWindow(Time.seconds(4)).reduce((r1,r2)=>(r1._1,r1._2+r2._2))

    data.print()

    env.execute("DemoReduce")

  }

}
