package com.joseph.streaming.api

import java.lang

import com.joseph.streaming.source.{MySourceFunction2, MySourceFunction3}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object DemoCoGroup {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.addSource(new MySourceFunction2).flatMap(_.split("\\W+")).map((_,1))
    val stream2 = env.addSource(new MySourceFunction3).flatMap(_.split("\\W+")).map((_,2))


    val data = stream1.coGroup(stream2).where(_._1).equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))

    val innerJoinFunction = data.apply(new InnerJoinFunction).name("InnerJoinedStream")
    innerJoinFunction.print().setParallelism(1)



    env.execute("DemoCoGroup")
  }


  class InnerJoinFunction extends CoGroupFunction[(String,Int),(String,Int),String] {
    override def coGroup(iterable: lang.Iterable[(String, Int)],
                         iterable1: lang.Iterable[(String, Int)], collector: Collector[String]): Unit = {
      //java集合类型转换成scala集合类型
      import scala.collection.JavaConverters._
      val t1 = iterable.asScala.toList
      val t2 = iterable1.asScala.toList


      if (t1.nonEmpty && t2.nonEmpty){
        for (t11<- t1){
          for (t22<-t2){
            collector.collect(t11._1+"---innerjoin---" + (t11._2+t22._2))
          }
        }
      }
    }
  }


}
