package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/29 15:40
  */
object DoubleValue02_union {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4)

    //3.2 创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(4 to 8)

    //3.3 计算两个RDD的并集
    rdd1.union(rdd2).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
