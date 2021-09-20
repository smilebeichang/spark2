package cn.sysu.day02

import java.awt.MultipleGradientPaint.CycleMethod

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @Author : song bei chang
  * @create 2021/3/24 7:19
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("xiao pang")

    //创建上下文
    val sc = new SparkContext(conf)

    //读取文件
    val fileRDD: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Spark\\input\\wordcount")

    //分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    //转换数据结构
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    //分组聚合
    val countRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    //采集到内存中
    val array: Array[(String, Int)] = countRDD.collect()

    //打印
    //array.foreach(println())
    //array.foreach()

    Thread.sleep(100000)

    sc.stop()



  }
}
