package cn.sysu.chapter07

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object $06_RangePartitioner {

  /**
    * RangePartitioner： 通过采样确定每个分区的边界,后续拿到数据的key之后对比边界知道数据应该放在哪个分区
    *     rangePartitioner分区间数据是有序的
    */
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    val rdd = sc.parallelize(List("aa"->1,"bb"->2,"aa"->5,"dd"->10,"bb"->15,"cc"->16,"aa"->1,"aa"->1,"aa"->1,"aa"->1,"aa"->1,"aa"->1,"aa"->1,"aa"->1,"aa"->1))

    //range
    val rdd3 = rdd.partitionBy(new RangePartitioner(2,rdd))

    rdd3.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    Thread.sleep(1000000000)
  }
}
