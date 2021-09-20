package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/9/20 19:44
  */
object value09_distinct {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val distinctRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    // 3.2 打印去重后生成的新RDD
    distinctRdd.distinct().collect().foreach(println)

    println("*"*100)

    // 3.3 对RDD采用多个Task去重，提高并发度
    distinctRdd.distinct(2).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
