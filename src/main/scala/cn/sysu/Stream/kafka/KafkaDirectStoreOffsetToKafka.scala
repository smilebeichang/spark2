package com.atguigu.sparksteaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

/**
 * Created by Smexy on 2021/7/3
 *
 *    借助kafka提供的api，手动将offset存储到kafka的 _consumer_offsets中，
  *    如果有幂等操作，此时是 精确一次，如果没有幂等操作，此时就是 最少一次
 *
 *
 *    核心： ①取消自动提交offset
 *          ②获取当前消费到的这批数据的offset信息
 *          ③进行计算和输出
 *          ④计算和输出完全完成后，再手动提交offset
 *
 *
 *   发生异常时提交offset会不会产生重复？
 *      发生了异常，是提交不了offset的！
 *

 */
object KafkaDirectStoreOffsetToKafka {


  def main(args: Array[String]): Unit = {

    val streamingContext = new StreamingContext("local[2]", "wc", Seconds(5))


    // 消费者的配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ecs2:9092,ecs3:9092,ecs4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0223",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    //指定要消费主题
    val topics = Array("topic2")

    // 使用提供的API，从kafka中获取一个DS
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )



    ds.foreachRDD { rdd =>

    // 判断当前RDD是不是KafkaRDD,如果是，获取其中的偏移量信息
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //OffsetRange(topic: 'topic2', partition: 3, range: [21（起始） -> 21(终止)])
      //在Driver端 ，在foreachRDD,只有RDD的算子，才在Executor端运行
     /* for (offset <- offsetRanges) {

        println(offset)
      }*/

      //计算逻辑   如果有幂等操作，此时是 精确一次，如果没有幂等操作，此时就是 最少一次
      rdd.map(record => {

        //模拟异常
      // if (record.value().equals("d"))  1 / 0

        // 将数据写到redis或hbase，保证写N次，结果不变
        (record.value(),1)
        }).reduceByKey(_+_)
        .collect().foreach(println(_))


      //手动提交offset
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamingContext.start()

    streamingContext.awaitTermination()


  }

}
