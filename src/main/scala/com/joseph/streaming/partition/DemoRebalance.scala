package com.joseph.streaming.partition

import com.joseph.streaming.source.MySourceFunction
import org.apache.flink.streaming.api.scala._

object DemoRebalance {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(3)

    val stream = env.addSource(new MySourceFunction)

    val streamRebalance = stream.rebalance

    streamRebalance.print()

    env.execute("DemoRebalance")
  }

}
