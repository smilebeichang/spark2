package cn.sysu.chapter07


import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class $07_ReadWrite {

  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

  @org.junit.Test
  def read(): Unit ={
    //读取文本数据
    //sc.textFile("datas/wc.txt")
    //读取序列化文件
    val rdd = sc.sequenceFile[Int,Int]("output/seq")

    println(rdd.collect().toList)

    //读取对象文件
    //println(sc.objectFile[Int]("output/obj").collect().toList)
  }

  @Test
  def write(): Unit ={
    val rdd = sc.parallelize(List(1,2,3,5,5,6,7))
    //保存为文本
    rdd.saveAsTextFile("output/text")

    //保存为对象文件
    rdd.saveAsObjectFile("output/obj")

    val rdd2 = rdd.map(x=>(x,x))

    rdd2.saveAsSequenceFile("output/seq")

    //rdd2.saveAsNewAPIHadoopFile()
    //rdd2.saveAsHadoopFile()
  }
}
