package cn.sysu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/3/17 22:45
  */
object depdence {

  var sc = new SparkContext(new SparkConf().setAppName("MySpark").setMaster("local[*]"))

  def main(args: Array[String]): Unit = {

    val fileRDD: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Spark\\input\\1.txt")
    println(fileRDD.dependencies)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)

    resultRDD.collect()
    resultRDD.saveAsTextFile("output4")
    Thread.sleep(10000000)
  }



}
