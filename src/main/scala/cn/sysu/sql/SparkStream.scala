package cn.sysu.sql

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @Author : song bei chang
  * @create 2021/3/20 23:11
  */
class  SparkStream {



  @org.junit.Test
  def hello2():Unit={
    // ①创建编程入口，StreamingContext
    val streamingContext = new StreamingContext("local[2]", "wc", Seconds(5))

    // ②创建编程模型： DStream  根据不同的数据源创建不同的DS
    // ReceiverInputDStream[String]: String 指一行内容
    val ds: ReceiverInputDStream[String] = streamingContext.socketTextStream("ecs4", 9999)


    // ③调用DS中的方法进行运算
    val result: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // ④调用行动算子，例如输出，打印等
    result.print(1000)

    // ⑤真正的计算，会在启动了app之后运行
    streamingContext.start()

    // ⑥流式应用，需要一直运行(类似agent) 不能让main运行完，阻塞main
    streamingContext.awaitTermination()
  }

  @org.junit.Test
  def hello1():Unit={
    //1.初始化Spark配置信息  local默认为1core,只能够运行executor
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.通过监控端口创建DStream，读进来的数据为一行行  居然读取不到数据
    val lineStreams = ssc.socketTextStream("ecs4", 9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
