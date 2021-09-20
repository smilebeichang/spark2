package cn.sysu.Stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author : song bei chang
  * @create 2021/5/4 12:41
  */
object UpdateStateByKey {


  def main(args: Array[String]) {


    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount").set("dfs.client.use.datanode.hostname", "true")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 设置ck
    // 报错 版本不匹配 Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.tracing.SpanReceiverHost
    ssc.checkpoint("./ck")

    // Create a DStream that will connect to hostname:port, like hadoop102:9999
    val lines = ssc.socketTextStream("ecs4", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = pairs.updateStateByKey[Int](
      //updateFunc
      (newValues: Seq[Int], state: Option[Int]) => Some(newValues.sum + state.getOrElse(0))
    )
    stateDstream.print()
    pairs.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //ssc.stop()
  }

}
