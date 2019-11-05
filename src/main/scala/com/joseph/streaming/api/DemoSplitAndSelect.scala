package com.joseph.streaming.api

import com.joseph.streaming.source.MySourceFunction
import org.apache.flink.streaming.api.scala._

object DemoSplitAndSelect {

  def main(args: Array[String]): Unit = {

    val env =  StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySourceFunction)

    val splitStream = stream.split(
      (num:Long)=>
        (num%2)match {
          case 0 => List("even")
          case 1 => List("odd")
        }
    )

    val evenstream = splitStream.select("even")

    evenstream.print().setParallelism(1)

    env.execute("DemoSplitAndSelect")
  }

}
