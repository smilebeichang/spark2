package cn.sysu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/3/18 12:25
  */
object ReaderWrite {

  var sc = new SparkContext(new SparkConf().setAppName("Spark_xiaopang").setMaster("local[*]"))
  var OUT_PATH = "output6"

  def main(args: Array[String]): Unit = {

    // 读取输入文件
    val inputRDD: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Hello\\output3\\part-00001")

    // 保存数据
    //inputRDD.saveAsTextFile(OUT_PATH)


    val dataRDD = sc.makeRDD(List((1,1),(2,2),(3,3)))

    // 保存数据为SequenceFile
    dataRDD.saveAsSequenceFile("output7")

    // 读取SequenceFile文件
    sc.sequenceFile[Int,Int]("output7").collect().foreach(println)
  }



}
