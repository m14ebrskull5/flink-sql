package org.example

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Union {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements("ha", "so", "fl")
    val ds2 = env.fromElements("ha", "so", "fl")
    val ds3 = env.fromElements(1, 2, 3)

    val res = ds1.connect(ds3)
    var res1 = res.map(new CoMapFunction[String, Int, String] {
      override def map1(str: String): String = {
        return "string" + str
      }

      override def map2(in2: Int): String = {

        return s"Long$in2"
      }
    })
    res1.print()
    env.execute()
  }
}
