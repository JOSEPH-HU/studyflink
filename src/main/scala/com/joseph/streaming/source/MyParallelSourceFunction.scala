package com.joseph.streaming.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}


class MyParallelSourceFunction extends ParallelSourceFunction[Int] {
  var count = 1
  var flag = true

  override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {

    while (flag){
      count += 1
      sourceContext.collect(count)
      Thread.sleep(200)

    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}
