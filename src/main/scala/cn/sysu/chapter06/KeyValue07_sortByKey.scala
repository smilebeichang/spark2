package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * @Author : song bei chang
  * @create 2021/9/20 20:27
  */
class KeyValue07_sortByKey {

  @Test
  def main(): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体的执行逻辑
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("b",3)))
    val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(true)
    val sortRDD2: RDD[(String, Int)] = dataRDD1.sortByKey(false)

    //3.1打印
    sortRDD1.collect().foreach(println)
    println("*"*100)
    sortRDD2.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
