package org.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Rebalance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var ds = env.fromSequence(0, 100)

  }
}
