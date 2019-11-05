package com.joseph.streaming.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext


class MySourceFunction2 extends SourceFunction[String]  {

  var count = 0L
  var flag = true

  override def run(sourceContext: SourceContext[String]): Unit = {

    while(flag){
      sourceContext.collect("hello jon")
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}
