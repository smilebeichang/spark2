package cn.sysu.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, _}

/**
  * @Author : song bei chang
  * @create 2021/5/28 16:23
  */
object Partition_file_default {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
    val sc: SparkContext = new SparkContext(conf)

    //1）默认分区的数量：默认取值为当前核数和2的最小值
    val rdd: RDD[String] = sc.textFile("input")

    println(rdd.partitions.length)

    rdd.saveAsTextFile("output")

    sc.stop()
  }


}
