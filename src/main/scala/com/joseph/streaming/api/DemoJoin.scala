package com.joseph.streaming.api

import java.lang

import com.joseph.streaming.source.{MySourceFunction2, MySourceFunction3}
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object DemoJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.addSource(new MySourceFunction2).flatMap(_.split("\\W+")).map((_,1))
    val stream2 = env.addSource(new MySourceFunction3).flatMap(_.split("\\W+")).map((_,1))

    val data = stream1.join(stream2).where(_._1).equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .apply(new InnerJoinFunction)


    data.print().setParallelism(1)

    env.execute("DemoJoin")
  }

  class InnerJoinFunction extends JoinFunction[(String,Int),(String,Int),(String,Int)] {
    override def join(in1: (String, Int), in2: (String, Int)): (String, Int) = {
      return (in1._1,in1._2+in2._2)
    }
  }

}
