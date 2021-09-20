package cn.sysu.chapter06

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/9/20 17:51
  */
object value07_filter {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val dataRDD = sc.makeRDD(List(
      1,2,3,4
    ),1)
    val dataRDD1 = dataRDD.filter(_%2 == 0)

    dataRDD1.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
