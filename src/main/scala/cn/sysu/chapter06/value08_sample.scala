package cn.sysu.chapter06

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * @Author : song bei chang
  * @create 2021/5/30 8:54
  */
object value08_sample {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[4]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = { // 随机算法相同，种子相同，那么随机数就相同


    val dataRDD = sc.makeRDD(List(
      1,2,3,4
    ),1)
    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取； 样本比例，抽样大小占中的大小
    // 第三个参数：随机数种子
    val dataRDD1 = dataRDD.sample(false, 0.5)


    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子,所以种子需要真随机
    val dataRDD2 = dataRDD.sample(true, 2)
  }

}
