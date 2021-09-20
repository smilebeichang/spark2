package cn.sysu.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @Author : song bei chang
  * @create 2021/5/28 15:13
  */
object Partition_default {


  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5))

    println(rdd.partitions.length)
    //3. 输出数据，产生了8个分区
    rdd.saveAsTextFile("output")

    //4.关闭连接
    sc.stop()
  }
}



