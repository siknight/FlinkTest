package wc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import wc.util.MyKafkaUtil

object KafkaApiStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource:FlinkKafkaConsumer011[String]  =MyKafkaUtil.getKafkaSource("first")

    val dstream: DataStream[String] = environment.addSource(kafkaSource)

    dstream.print()

    environment.execute()
  }

}
