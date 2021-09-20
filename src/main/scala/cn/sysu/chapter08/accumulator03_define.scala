package cn.sysu.chapter08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/6/3 23:18
  */
object accumulator03_define {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)

    //3.1 创建累加器
    val acc: WordCountAccumulator = new WordCountAccumulator()

    //3.2 注册累加器
    sc.register(acc,"wordcount")

    //3.3 使用累加器
    rdd.foreach(
      word =>{
        acc.add(word)
      }
    )

    //3.4 获取累加器的累加结果
    println(acc.value)

    //4.关闭连接
    sc.stop()
  }

}
