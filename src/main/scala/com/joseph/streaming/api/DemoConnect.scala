package com.joseph.streaming.api

import com.joseph.streaming.source.{MySourceFunction2, MySourceFunction3}
import org.apache.flink.streaming.api.scala._

object DemoConnect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.addSource(new MySourceFunction2).flatMap(_.split("\\W+")).map((_,1))
    val stream2 = env.addSource(new MySourceFunction3).flatMap(_.split("\\W+")).map((_,2))

    val connectedStream = stream1.connect(stream2)

    //两个流合并以后单独处理
    val result = connectedStream.map((_:(String,Int))=>true,(_:(String,Int))=>false)

    result.print().setParallelism(1)

    env.execute("DemoConnect")
  }

}
