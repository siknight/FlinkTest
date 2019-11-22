package wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
  * Flink离线数据处理,打包到linux离线版
  */
object WordCount2 {
  //一共分为四步  1.env,创建执行flink环境 2.source，创建数据来源
  // 3.transform 转换 4.sink  执行想要结果
  def main(args: Array[String]): Unit = {
    //从外部命令中获取参数，flink自带的一个参数处理工具
    val tool:ParameterTool = ParameterTool.fromArgs(args);
    val inputpath:String = tool.get("input");
    val outputpath:String = tool.get("output");

    //1.env
    val env:ExecutionEnvironment =ExecutionEnvironment.getExecutionEnvironment
    //2.source
    val txtDataSet:DataSet[String]=env.readTextFile(inputpath);
    // 3.transform 转换
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    val aggSet:AggregateDataSet[(String,Int)] =txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1);

    //4.sink  setParallelism是设置并行度，
    // 如果最后只想输出一个文件，并行度就设置为1
    aggSet.writeAsCsv(outputpath).setParallelism(1);
    env.execute();

  }


}
