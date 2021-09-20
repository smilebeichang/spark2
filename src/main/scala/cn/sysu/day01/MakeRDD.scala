package cn.sysu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/2 15:00
  * 创建RDD的四种方式：
  *    1. 从集合（内存）中创建RDD  parallelize/makeRDD
  *    2. 从外部存储（文件）创建RDD
  *    3. 从其他RDD创建
  *    4. 直接创建RDD（new）
  */
object MakeRDD {



  val INPUT_PATH = "E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Spark\\input\\wordcount"

  def main(args: Array[String]): Unit = {
    parallelize_makeRDD()
    textFile()

  }


  def parallelize_makeRDD(): Unit ={

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd1 = sparkContext.parallelize(
      List(1,2,3,4)
    )
    val rdd2 = sparkContext.makeRDD(
      List(1,2,3,4)
    )
    // 将数据聚合结果采集到内存中
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    sparkContext.stop()

  }

  def textFile(): Unit ={

    val sparkConf =  new SparkConf().setMaster("local[*]").setAppName("xiao-pang")
    val sparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sparkContext.textFile(INPUT_PATH)
    fileRDD.collect().foreach(println)
    sparkContext.stop()

  }


}
