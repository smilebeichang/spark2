package cn.sysu.chapter07

import org.apache.spark.{SparkConf, SparkContext}

object $08_Accumulator {

  //累加器: 将每个task的累加结果返回给Driver,由Driver统一汇总
  //     好处: 可以在一定程度上减少shuffle
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    val acc = sc.longAccumulator("xx")
    var sum = 0

    val rdd = sc.parallelize(List(10,2,6,8,3,4,9))

    rdd.foreach(x=> acc.add(x))

    println(acc.value)

    Thread.sleep(100000000)

  }
}
