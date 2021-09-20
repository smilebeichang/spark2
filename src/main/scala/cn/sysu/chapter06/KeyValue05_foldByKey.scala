package cn.sysu.chapter06

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 11:40
  */
object KeyValue05_foldByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val list: List[(String, Int)] = List(("a",1),("a",1),("a",1),("b",1),("b",1),("b",1),("b",1),("a",1))
    val rdd = sc.makeRDD(list,2)

    //3.2 求wordcount
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    println('#'*100)
    //'#'的hashCode 为35
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
