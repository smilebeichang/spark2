package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 15:52
  */
object action06_takeOrdered {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[4]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,2,4))

    //3.2 返回RDD中排完序后的前两个元素
    val result: Array[Int] = rdd.takeOrdered(2)
    println(result.mkString(","))

    //4.关闭连接
    sc.stop()
  }

}
