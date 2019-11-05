package com.joseph.streaming.partition

import com.joseph.streaming.source.MySourceFunction
import org.apache.flink.streaming.api.scala._

object DemoShuffle {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySourceFunction)

    val shuffleStream = stream.shuffle

    shuffleStream.print()

    env.execute("DemoShuffle")

  }

}
