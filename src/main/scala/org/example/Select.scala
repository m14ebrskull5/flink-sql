package org.example

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object Select {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var ds = env.fromElements(1,2,3,4,5,6,7,8,9)
    var oddTag = new OutputTag[Int]("奇数")
    var evenTag = new OutputTag[Int]("偶数")

    var res = ds.process(new ProcessFunction[Int, Int] {
      override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
        if(i % 2 == 0) context.output(evenTag, i) else context.output(oddTag, i)
      }
    })
    var oddds = res.getSideOutput(oddTag)
    var eveds = res.getSideOutput(evenTag)

    oddds.print("奇数")
    eveds.print("偶数")
    env.execute()
  }
}
