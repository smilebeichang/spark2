package com.atguigu.sparksteaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Created by Smexy on 2021/7/3
 *
 *
 *    数据处理语义可以判断出一个系统的特点
 *
 *        at least once : 至少一次，可以是 1次或多次，存在重复处理！
 *        at most once(没人用) ：  至多一次，可以是0次或1次， 存在丢数据风险！
 *        exactly once ：  精确一次，不多不少
  *
 *            at least once  + 去重  = exactly once
 *
 *
 *            演示丢数据
 *
 *            原因：  目前是自动提交offset，因此程序运行到49时，只是从kafka消费到了数据，还没有进行计算，就已经自动提交了offset!
 *                        无论发生什么情况，都只能从提交的offset后，再消费！
 *
 *                        如果程序异常，异常期间的那批数据，也无法在程序重启后，再次消费到！
 *
 */
object KafkaDirectLoseDataTest {

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

    //指定要消费主题
    val topics = Array("topic_log")

    // 使用提供的API，从kafka中获取一个DS
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //程序的运算逻辑
    val ds1: DStream[String] = ds.map(record => {

      //模拟异常
      if (record.value().equals("d"))  1 / 0

      record.value()

    })

    ds1.print()


    streamingContext.start()

    streamingContext.awaitTermination()


  }

}
