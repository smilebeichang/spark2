package cn.sysu.chapter06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author : song bei chang
  * @create 2021/5/29 15:48
  */
object DoubleValue04_zip {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3),3)

    //3.2 创建第二个RDD
    val rdd2: RDD[String] = sc.makeRDD(Array("a","b","c"),3)

    //3.3 第一个RDD组合第二个RDD并打印
    rdd1.zip(rdd2).collect().foreach(println)

    //3.4 第二个RDD组合第一个RDD并打印
    rdd2.zip(rdd1).collect().foreach(println)

    //3.5 创建第三个RDD（与1，2分区数不同）
    val rdd3: RDD[String] = sc.makeRDD(Array("a","b"), 3)

    //3.6 元素个数不同，不能拉链
    // Can only zip RDDs with same number of elements in each partition
    rdd1.zip(rdd3).collect().foreach(println)

    //3.7 创建第四个RDD（与1，2分区数不同）
    val rdd4: RDD[String] = sc.makeRDD(Array("a","b","c"), 2)

    //3.8 分区数不同，不能拉链
    // Can't zip RDDs with unequal numbers of partitions: List(3, 2)
    rdd1.zip(rdd4).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}