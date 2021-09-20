package cn.sysu

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * @Author : song bei chang
  * @create 2021/9/20 16:34
  */
package object day01 {

  // scc 自定义live template
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //4.关闭连接
    sc.stop()
  }

  // aa 自定义live template
  @Test
  def hello():Unit={

  }

}
