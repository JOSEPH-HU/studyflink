package com.joseph.streaming.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}


class MyRichParallelSourceFunction extends RichParallelSourceFunction[Int] {
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

  override def open(parameters: Configuration): Unit = super.open(parameters)


  override def close(): Unit = super.close()
}
