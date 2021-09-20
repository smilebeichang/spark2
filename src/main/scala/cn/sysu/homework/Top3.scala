package cn.sysu.homework

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/31 18:11
  */
object Top3 {


  //每个省份点击量最多的三个广告
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    //1、读取数据
    val rdd1 = sc.textFile("input/agent.log")
    //2、是否过滤 是否去重 是否列裁剪【省份、广告】
    /*val rdd2 = rdd1.map(line=>{
      val arr = line.split(" ")
      val province = arr(1)
      val adid = arr.last
      (province,adid)
    })
    //3、按照省份分组
    val rdd3 = rdd2.groupByKey()
    //4、统计每个省份每个广告点击多少次
   val rdd4 = rdd3.map(x=>{
      //x = 广东省->List(1,6,3,2,1,9,3,2)
      val province = x._1
      val adgrouped = x._2.groupBy(y=>y)
      val adMap = adgrouped.map(y=>(y._1,y._2.size))
      //5、对每个省份所有广告点击次数排序,取前三
      val top3 = adMap.toList.sortBy(_._2).reverse.take(3)
      (province,top3)
    })*/

    //2、是否过滤 是否去重 是否列裁剪【省份、广告】
    val rdd2 = rdd1.map(line=>{
      val arr = line.split(" ")
      ( (arr(1),arr.last),1 )
    })

    //3、统计每个省份每个广告点击次数
    val rdd3 = rdd2.reduceByKey(_+_)
    //4、按照省份分组
    val rdd4 = rdd3.groupBy{
      case ((province,adid),num) => province
    }
    //5、排序取前三
    val rdd5 = rdd4.map(x=>{
      val top3 = x._2.toList.sortBy(_._2).reverse.take(3)
      (x._1, top3)
    })

    //6、结果展示
    rdd5.collect().foreach(println(_))
  }


}
