package wc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}



/**
  * fink实时计算
  */
object StreamWcApp {

  def main(args: Array[String]): Unit = {
    //1.env  要用StreamExecutionEnvironment
    val env:StreamExecutionEnvironment  =StreamExecutionEnvironment.getExecutionEnvironment
    //2.source  要用DataStream
    val txtDataSet:DataStream[String]=env.socketTextStream("hadoop102",7777);

    // flatMap和Map需要引用的隐式转换 keyBy(0) 0表示map的第一个参数 sum(1) 表示map的第二个参数
    import org.apache.flink.api.scala._
    val dStream:DataStream[(String,Int)] =txtDataSet.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1);

    //打印
    dStream.print()

    env.execute()
  }


}
