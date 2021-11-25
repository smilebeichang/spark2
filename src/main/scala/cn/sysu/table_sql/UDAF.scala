package cn.sysu.table_sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit


/**
  * @Author : song bei chang
  * @create 2021/5/3 21:34
  *        需求：计算平均工资
  */
class UDAF {

  // 1)实现方式 - RDD
  @junit.Test
  def hello():Unit={
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val res: (Int, Int) = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40))).map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    println(res._1/res._2)
    // 关闭连接
    sc.stop()
  }

  // 2)实现方式 - 累加器
  @Test
  def hello2():Unit={

    class MyAC extends AccumulatorV2[Int,Int]{
      var sum:Int = 0
      var count:Int = 0
      override def isZero: Boolean = {
        return sum ==0 && count == 0
      }

      override def copy(): AccumulatorV2[Int, Int] = {
        val newMyAc = new MyAC
        newMyAc.sum = this.sum
        newMyAc.count = this.count
        newMyAc
      }

      override def reset(): Unit = {
        sum =0
        count = 0
      }

      override def add(v: Int): Unit = {
        sum += v
        count += 1
      }

      override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        other match {
          case o:MyAC=>{
            sum += o.sum
            count += o.count
          }
          case _=>
        }

      }

      override def value: Int = sum/count
    }
  }

  // 3)实现方式 - UDAF - 弱类型
  @Test
  def hello3():Unit={

    /*
      定义类继承UserDefinedAggregateFunction，并重写其中方法
    */
    class MyAveragUDAF extends UserDefinedAggregateFunction {

      // 聚合函数输入参数的数据类型
      def inputSchema: StructType = StructType(Array(StructField("age",IntegerType)))

      // 聚合函数缓冲区中值的数据类型(age,count)
      def bufferSchema: StructType = {
        StructType(Array(StructField("sum",LongType),StructField("count",LongType)))
      }

      // 函数返回值的数据类型
      def dataType: DataType = DoubleType

      // 稳定性：对于相同的输入是否一直返回相同的输出。
      def deterministic: Boolean = true

      // 函数缓冲区初始化
      def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 存年龄的总和
        buffer(0) = 0L
        // 存年龄的个数
        buffer(1) = 0L
      }

      // 更新缓冲区中的数据
      def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
        if (!input.isNullAt(0)) {
          buffer(0) = buffer.getLong(0) + input.getInt(0)
          buffer(1) = buffer.getLong(1) + 1
        }
      }

      // 合并缓冲区
      def merge(buffer1: MutableAggregationBuffer,buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      }

      // 计算最终结果
      def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
    }

    //。。。

//    //创建聚合函数
//    var myAverage = new MyAveragUDAF
//
//    //在spark中注册聚合函数
//    spark.udf.register("avgAge",myAverage)
//
//    spark.sql("select avgAge(age) from user").show()

  }



}
