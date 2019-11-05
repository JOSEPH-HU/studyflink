package com.joseph.streaming.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext


class MySourceFunction extends SourceFunction[Long]  {

  var count = 0L
  var flag = true

  override def run(sourceContext: SourceContext[Long]): Unit = {

    while(flag){
      count += 1
      sourceContext.collect(count)
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}
