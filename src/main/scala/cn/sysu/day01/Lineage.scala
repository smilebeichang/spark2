package cn.sysu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/3/17 22:41
  */
object Lineage {

  var sc = new SparkContext(new SparkConf().setAppName("MySpark").setMaster("local[*]"))

  def main(args: Array[String]): Unit = {

    val fileRDD: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Spark\\input\\1.txt")
    println(fileRDD.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)

    resultRDD.collect()
  }

}
