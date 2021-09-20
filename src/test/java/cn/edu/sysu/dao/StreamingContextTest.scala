package cn.edu.sysu.dao

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit._

/**
 * Created by Smexy on 2021/7/2
 */
class StreamingContextTest {


  /*
    def this(
      master: String,
      appName: String,
      batchDuration: Duration）
          batchDuration： 将流式数据，划分为若干个批次的时间间隔。采集周期！

          case class Duration (private val millis: Long)
              new Duration(millis)

          还可以调用
object Milliseconds {
  def apply(milliseconds: Long): Duration = new Duration(milliseconds)
}

object Seconds {
  def apply(seconds: Long): Duration = new Duration(seconds * 1000)
}

object Minutes {
  def apply(minutes: Long): Duration = new Duration(minutes * 60000)
}

   */
    @Test
      def testBaseUrlAndAppName():Unit={


      val streamingContext = new StreamingContext("local[*]", "wc", Seconds(5))

      println(streamingContext)

      }



  @Test
  def testBaseSparkConf():Unit={


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    println(streamingContext)

  }

  @Test
  def testBaseSparkContext():Unit={


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")

    val sparkContext = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    println(streamingContext)

    //获取streamingContext 关联的 SparkContext

    println(streamingContext.sparkContext == sparkContext)

  }

  // 在同一个虚拟机中只允许运行一个SparkContext，在创建StreamingContext时，内置一个SparkContext
  @Test
  def testAttention():Unit={


    //org.apache.spark.SparkException: Only one SparkContext should be running in this JVM
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")

    // 第一个SparkContext
    val sparkContext = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext("local[*]", "wc", Seconds(5))

    //第二个sparkcontext
    streamingContext.sparkContext


  }

  // 在同一个虚拟机中只允许运行一个SparkContext，在创建StreamingContext时，内置一个SparkContext
  // 在一个JVM中，所有的DStream都是从同一个StreamingContext 创建的，它们的 batchDuration 都是一样的！
  @Test
  def testAttention2():Unit={


    //org.apache.spark.SparkException: Only one SparkContext should be running in this JVM
    val streamingContext1 = new StreamingContext("local[*]", "wc", Seconds(5))

    val streamingContext2 = new StreamingContext("local[*]", "wc", Seconds(3))



  }





}
