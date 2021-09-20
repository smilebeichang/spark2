package com.atguigu.sparksteaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Created by Smexy on 2021/7/3
 *
 *    不能自动提交offset，需要自己维护offset!
 *
 *    如何自己维护？
 *        三种方式：
 *            ①Checkpoints
 *            ②Kafka itself
 *            ③Your own data store
 *
 *
 *     Checkpoints：  checkpoint本质是一个持久化的文件系统！
 *                    将kafka的偏移量存储在 spark提供的ck目录中，下次程序重启时，会从ck目录获取上次消费的offset，继续消费！
 *
 *                    RDD.cache()
 *                    RDD.checkpoint---->ReliableRDDCheckpointData
 *                        This allows drivers to be restarted on failure with previously computed state.
 *
 *                        app运行报错了，但是你设置了ck目录，在异常时，程序会将一部分状态(想保存的数据，例如消费者消费的offset)保存到一个ck目录中，之后app重启后，driver从ck目录中读取状态，恢复！
 *
 *
 *          操作： ①设置ck目录
 *                     streamingContext.checkpoint("kafkack")
 *                ②设置故障的时候，让Driver从ck目录恢复
 *
 *          def getActiveOrCreate(
 *                checkpointPath: String,       //ck目录
 *                creatingFunc: () => StreamingContext      // 一个空参的函数，要求返回StreamingContext，函数要求把计算逻辑也放入次函数
 *
 *              ): StreamingContext
 *
 *                ③取消自动提交offset
 *                    "enable.auto.commit" -> "false"
 *
 *
 *
 *
 *                    要不要设置发生故障时哪些数据保存到ck目录?
 *                        不用设置，也设置不了，spark自动实现！
 *
 *
 *                    使用checkpoint 输入端必须是精准一次吗？
 *                        输入端精准一次？
 *
 *              Checkpoints的弊端：
 *                  ①一般的异常，会catch住，继续运行，不给你异常后，从异常位置继续往后消费的机会
 *                  ②重启后，会从上次ck目录记录的时间戳，一直按照 slide时间，提交Job，到启动的时间
 *                  ③会产生大量的小文件
 *
 *
 *                  不推荐使用！
 *
 *
 *                  at least once :  不能保证精确一次！
 *
 *
 */
object KafkaDirectCKTest {

  val ckPath:String ="hdfs://ecs2:9820/ck"

  def main(args: Array[String]): Unit = {


    def creatingStreamingContextFunc() : StreamingContext={

      val streamingContext = new StreamingContext("local[2]", "wc", Seconds(5))

      //设置ck目录
      streamingContext.checkpoint(ckPath)


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
        if (record.value().equals("d"))  throw new UnknownError("故障了！求你婷下来吧！")

        record.value()

      })

      ds1.print()

      streamingContext


    }




    // 要么直接创建，要么从ck目录中恢复一个StreamingContext
    val context: StreamingContext = StreamingContext.getActiveOrCreate(ckPath, creatingStreamingContextFunc)


    context.start()

    context.awaitTermination()




  }

}
