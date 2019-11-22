package wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
  * Flink离线数据处理
  */
object WorldCount {
  //一共分为四步  1.env,创建执行flink环境 2.source，创建数据来源
    // 3.transform 转换 4.sink  执行想要结果
  def main(args: Array[String]): Unit = {

    //1.env
    val env:ExecutionEnvironment =ExecutionEnvironment.getExecutionEnvironment
    //2.source
    val txtDataSet:DataSet[String]=env.readTextFile("D:\\testinput\\a.txt");
    // 3.transform 转换
      // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    val aggSet:AggregateDataSet[(String,Int)] =txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1);

    //4.sink
    aggSet.print();

  }

}
