package org.example

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.Random



object ExampleScala {
  case class Student(id:Int, name:String,age:Int)
  def main(args: Array[String]): Unit = {
    var conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var ds = env.addSource(new RichParallelSourceFunction[Student] {
      private var flag = true
      private var connection : Connection = null
      private var ps : PreparedStatement = null
      private var rs : ResultSet = null
      override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
        while (flag) {
          rs = ps.executeQuery()
          while (rs.next()) {
            sourceContext.collect(Student(rs.getInt(1), rs.getString(2), rs.getInt(3)))

          }
          Thread.sleep(4000)
        }
      }

      override def cancel(): Unit = {
        flag = false
      }
      //只执行一次，适合开启资源
      override def open(parameters: Configuration): Unit =  {
        super.open(parameters)
        connection = DriverManager.getConnection("jdbc:mysql://192.168.38.141:30200/zz-bigdata", "root", "liubei@2021")
        val sql = "select id,name,age from t_student"
        ps = connection.prepareStatement(sql)

      }

      override def close(): Unit = {
        if (connection != null) {
          connection.close()
        }
      }
    }).setParallelism(1)
//    var dsm = ds.map(i => i * i)
    ds.print()
    env.execute()
//    myLoop(start = 4, end = 32) {
//      println(_)
//    }
//    myLoop(1,3)(println(_))
  }
  def myLoop(start: Int, end: Int)(callback: Int => Unit) = {
    for (i <- Range(start, end)) {
      callback(i)
    }

  }

  def myLoop1(start: Int, end: Int, callback: Int => Unit) = {
    for (i <- Range(start, end)) {
      callback(i)
    }

  }
  def hello(title: String, firstName:String, lastNameOpt: Option[String]) = {
    println(lastNameOpt)
//    lastNameOpt match {
//      case Some(lastName) => println(s"Hello $title")
//      case None => println(s"Hello $firstName")
//    }
  }

  def hello2(name: Option[String]) = {
    for (s <- name) println(s"Hello $s")
  }

}
