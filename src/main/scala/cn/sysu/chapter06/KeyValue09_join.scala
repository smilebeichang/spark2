package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 14:52
  */
object KeyValue09_join {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "d"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))

    //3.3 join操作并打印结果
    println("===========  join  ===============")
    val rdd2: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    rdd2.collect().foreach(println)

    println("===========  leftOuterJoin  ===============")
    val rdd3: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    rdd3.collect().foreach(println)

    println("===========  rightOuterJoin  ===============")
    val rdd4: RDD[(Int, (Option[String], Int))] = rdd.rightOuterJoin(rdd1)
    rdd4.collect().foreach(println)

    println("===========  fullOuterJoin  ===============")
    val rdd5: RDD[(Int, (Option[String], Option[Int]))] = rdd.fullOuterJoin(rdd1)
    rdd5.collect().foreach(println)



    //4.关闭连接
    sc.stop()
  }

}
