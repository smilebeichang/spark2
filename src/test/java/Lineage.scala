import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test

/**
  * @Author : song bei chang
  * @create 2021/5/3 0:45
  */
class Lineage {

  var sc = new SparkContext(new SparkConf().setAppName("XiaoPang").setMaster("local[*]"))

  @Test
  def hello():Unit={
    val fileRDD: RDD[String] = sc.textFile("input/1.txt")
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

  @Test
  def hello1():Unit={

    val fileRDD: RDD[String] = sc.textFile("input/1.txt")
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
  }

}
