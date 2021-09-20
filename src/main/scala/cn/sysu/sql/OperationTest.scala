package cn.sysu.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before}

/**
  * @Author : song bei chang
  * @create 2021/3/17 21:20
  */
class OperationTest {

  var sc = new SparkContext(new SparkConf().setAppName("MySpark").setMaster("local[*]"))
  var OUT_PATH = "output"

  @Before
  def init: Unit ={
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(OUT_PATH)

    //如果目录存在,则删除
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }

  @After
  def stop(): Unit ={
    sc.stop()
  }

  @Test
  def hello1():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)

    // 聚合数据
    val reduceResult: Int = rdd.reduce(_+_)
    println(reduceResult)

  }
  
  @Test
  def hello2():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 收集数据到Driver
    rdd.collect().foreach(println)
    
  }

  @Test
  def hello3():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 返回RDD中元素的个数
    val countResult: Long = rdd.count()
    println(countResult)

  }
  
  @Test
  def hello4():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(5,1,2,3,4))

    // 返回RDD中元素的个数
    val firstResult: Int = rdd.first()
    println(firstResult)
    
  }
  
  @Test
  def hello5():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 返回RDD中元素的个数
    val takeResult: Array[Int] = rdd.take(2)
    println(takeResult.mkString(","))
    
  }
  
  @Test
  def hello6():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,2,4))

    // 返回RDD中元素的个数
    val result: Array[Int] = rdd.takeOrdered(2)
    println(result(0)+ " "+(result(1)))
    
  }

  @Test
  def hello7():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)

    // 将该RDD所有元素相加得到结果
    val result: Int = rdd.aggregate(0)(_ + _, _ + _)
    //val result: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result)

  }

  @Test
  def hello8():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val foldResult: Int = rdd.fold(0)(_+_)
    println(foldResult)

  }
  
  @Test
  def hello9():Unit={
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    // 统计每种key的个数
    val result: collection.Map[Int, Long] = rdd.countByKey()
    println(result)
    
  }
  
  @Test
  def hello10():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 保存成Text文件
    rdd.saveAsTextFile("output")

    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")

    // 保存成Sequencefile文件
    rdd.map((_,1)).saveAsSequenceFile("output2")
    
  }

  @Test
  def hello11():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 收集后打印
    rdd.map(num=>num).collect().foreach(println)

    println("****************")

    // 分布式打印
    rdd.foreach(println)

  }



}
