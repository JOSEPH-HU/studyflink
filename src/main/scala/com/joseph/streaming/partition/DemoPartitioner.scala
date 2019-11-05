package com.joseph.streaming.partition

import com.joseph.streaming.source.MySourceFunction
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object DemoPartitioner {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream = env.addSource(new MySourceFunction).map((_,99))

    val steamParttitioner = stream.partitionCustom(new MyPartitioner,0)

    steamParttitioner.print()

    env.execute("DemoPartitioner")


  }

  class MyPartitioner extends Partitioner[Long] {
    override def partition(k: Long, i: Int): Int = {
      k.toInt%i
    }
  }

}
