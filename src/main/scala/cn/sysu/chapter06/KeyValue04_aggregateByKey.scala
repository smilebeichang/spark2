package cn.sysu.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 11:29
  */
object KeyValue04_aggregateByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //3.2 取出每个分区相同key对应值的最大值，然后相加
    rdd.aggregateByKey(0)(math.max(_, _), _ + _).collect().foreach(println)

    println("="*100)

    /** @note   其他方式 */
    val rddot =
      sc.makeRDD(List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      ),2)
    // 0:("a",1),("a",2),("c",3) => (a,10)(c,10)
    //                                         => (a,10)(b,10)(c,20)
    // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)


    val resultRDD: RDD[(String, Int)] = rddot.aggregateByKey(10)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    resultRDD.collect().foreach(println)

    System.out.println("*"*100)
    val dataRDD1 =
      sc.makeRDD(List(("a",1),("a",1),("b",2),("c",3)))
    val dataRDD2 =
      dataRDD1.aggregateByKey(0)(_+_,_+_)

    dataRDD2.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }


}
