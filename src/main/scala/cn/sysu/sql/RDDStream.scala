package cn.sysu.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Author : song bei chang
  * @create 2021/7/2 11:36
  */
class RDDStream {

  // object @Test无法直接使用
  // Test class should have exactly one public constructor


  @org.junit.Test
  def hello():Unit={

    //1.初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))

    //3.创建RDD队列  import scala.collection.mutable
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建QueueInputDStream
    // @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)

    //5.处理队列中的RDD数据
    val mappedStream = inputStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //6.打印结果
    reducedStream.print(5)

    //7.启动任务
    ssc.start()

    //8.循环创建并向RDD队列中放入RDD  居然没有顺序 一直以为需要放到rddQueue之前,原因:批处理
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 30, 4)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}
