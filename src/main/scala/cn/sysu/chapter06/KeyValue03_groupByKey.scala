package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 10:09
  */
object KeyValue03_groupByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //3.2 将相同key对应值聚合到一个Seq中
    val group: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    //3.3 打印结果
    group.collect().foreach(println)

    println("*"*100)
    //3.4 计算相同key对应值的相加结果
    group.map(t=>(t._1,t._2.sum)).collect().foreach(println)


    /** @note  其他方式的groupBy */
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = dataRDD1.groupByKey()
    val dataRDD3 = dataRDD1.groupByKey(2)
    val dataRDD4 = dataRDD1.groupByKey(new HashPartitioner(2))

    //打印结果
    println("^"*100)
    dataRDD2.collect().foreach(println)
    println("*"*100)
    dataRDD3.collect().foreach(println)
    println("="*100)
    dataRDD4.collect().foreach(println)










    //4.关闭连接
    sc.stop()
  }

}
