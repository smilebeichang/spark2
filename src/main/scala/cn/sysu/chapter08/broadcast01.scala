package cn.sysu.chapter08

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/6/3 23:59
  */
object broadcast01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.采用集合的方式，实现rdd1和list的join
    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val list: String = "WARN"

    // 声明广播变量
    val warn: Broadcast[String] = sc.broadcast(list)

    val filter: RDD[String] = rdd.filter {
      // log=>log.contains(list)
      log => log.contains(warn.value)
    }

    filter.foreach(println)

    Thread.sleep(100000)

    //4.关闭连接
    sc.stop()
  }


}
