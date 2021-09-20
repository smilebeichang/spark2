package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/29 11:11
  */
object value03_mapPartitionsWithIndex {


  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 8)

    // 3.2 创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    val indexRdd = rdd.mapPartitionsWithIndex( (index,items)=>{items.map( (index,_) )} )

    // 3.3 打印修改后的RDD中数据
    indexRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
