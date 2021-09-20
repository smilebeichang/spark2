package com.atguigu.sparksteaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Smexy on 2021/7/3
 *
 *    Receiver 模式：   接收器模式！ 使用Receiver接受kafka中的数据，之后当Task调度到某个具体的Executor,
 *                      Executor 会拷贝 Receiver接受到的数据到当前的Executor上，之后再计算！
 *
 *
 *    Direct模式(推荐)：  直连模式！  没有Receiver，由 Task所在的Executor直接从kafka消费数据，之后运算！
 *
 *    官方文档： http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 *
 *
 *    spark-streaming-kafka-0-10_2.12 对比之前的版本提供的功能的特色：
 *            ①提供了更为简单的并行度设置
 *                  kafka的分区  和  Spark中RDD的分区是1：1的关系
 *
 *                  kafka的分区越多，RDD的分区数越多，并行度高
 *
 *            ②提供了对元数据,offset的访问
 *
 *
 *    消费kafka的数据需要：
 *          ①消费者的配置  ：
 *                Key-value的反序列化器
 *                集群地址
 *                消费者组id
 *                消费者id(可选)
 *                是否自动提交offset，如果要自动提交，多久自动提交1次
 *
 *
 *                auto.offset.reset: 每次启动消费者，从哪个位置开始消费
 *                    latest： 从最新的位置消费
 *
 *
 *
 * def createDirectStream[K, V](
 *    ssc: StreamingContext,                   //streamingContext
 *    locationStrategy: LocationStrategy,
 *            位置策略：  指 Task允许的Executor 和 kafka的broker 的网络拓扑关系
 *            大部分情况，
 *                            PreferConsistent ： 大部分情况，公司用
 *                            PreferBrokers：  executor和broker在同一台机器
 *                            PreferFixed： 自定义
 *
 *
 *    consumerStrategy: ConsumerStrategy[K, V]
 *          消费策略
 *              独立消费者：       不需要借助Kafka集群保存Offset
 *                                      assign
 *              非独立消费者（大部分）：      需要kafka集群，采取分配策略为消费者组的每个消费者分配分区！
 *                                    需要借助kafka集群保存offset
 *                                      subscribe
 * ): InputDStream[ConsumerRecord[K, V]]
 *
 */
object KafkaDirectModeDemo {

  def main(args: Array[String]): Unit = {

    val streamingContext = new StreamingContext("local[2]", "wc", Seconds(5))


    // 消费者的配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ecs2:9092,ecs3:9092,ecs4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0223",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true"
    )

    // 指定要消费主题
    val topics = Array("topic_log")

    // 使用提供的API，从kafka中获取一个DS
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val ds1: DStream[String] = ds.map(record => record.value())

    ds1.print()


    streamingContext.start()

    streamingContext.awaitTermination()


  }

}
