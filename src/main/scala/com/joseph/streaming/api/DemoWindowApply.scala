package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction2
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object DemoWindowApply {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySourceFunction2)

    val data = stream.flatMap(_.split("\\W+")).map((_,1)).keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(4)))
     // .apply(new MyWindowFunction())
  }

 /* class MyWindowFunction extends WindowFunction[(String,Int),Int,String,TumblingProcessingTimeWindows] {
    override def apply(key: String, window: TumblingProcessingTimeWindows, input: Iterable[(String, Int)], out: Collector[Int]): Unit = {
      var count = 0
      for(i<-input){
        count = count + i._2
      }
      out.collect(count)
    }*/
  //}

}
