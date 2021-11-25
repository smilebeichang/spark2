package cn.sysu.table_sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before}

/**
  * @Author : song bei chang
  * @create 2021/5/3 0:16
  */
class Action {

  var sc = new SparkContext(new SparkConf().setAppName("XiaoPang").setMaster("local[*]"))
  var OUT_PATH = "output"

  @Before
  def init(): Unit ={
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(OUT_PATH)

    //如果目录存在,则删除
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }

  @After
  def stop():Unit={
    sc.stop()
  }

  @Test
  def hello1():Unit={
    //聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 聚合数据
    val reduceResult: Int = rdd.reduce(_+_)
    println(reduceResult)
  }

  @Test
  def hello2():Unit={
    //在驱动程序中，以数组Array的形式返回数据集的所有元素
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 收集数据到Driver
    val ints: Array[Int] = rdd.collect()
    ints.foreach(println)
  }

  @Test
  def hello3():Unit={
    //返回RDD中元素的个数
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,4))

    // 返回RDD中元素的个数
    val countResult: Long = rdd.count()
    println(countResult)
  }

  @Test
  def hello4():Unit={
    //返回RDD中的第一个元素
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 返回RDD中元素的个数
    val firstResult: Int = rdd.first()
    println(firstResult)
  }

  @Test
  def hello5():Unit={
    //返回一个由RDD的前n个元素组成的数组
    val rdd: RDD[Int] = sc.makeRDD(List(1,5,2,3,4))

    // 返回RDD中元素的个数
    val takeResult: Array[Int] = rdd.take(2)
    println(takeResult.mkString(","))
  }

  @Test
  def hello6():Unit={
    //返回该RDD排序后的前n个元素组成的数组
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,2,4))

    // 返回RDD中元素的个数
    val result: Array[Int] = rdd.takeOrdered(2)
    println(result.mkString(","))
  }

  @Test
  def hello7():Unit={
    //分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)

    // 将该RDD所有元素相加得到结果
    val result1: Int = rdd.aggregate(0)(_ + _, _ + _)
    val result2: Int = rdd.aggregate(10)(_ + _, _ + _)
    println("result1: "+result1 + "  result2:"+ result2)

  }

  @Test
  def hello8():Unit={
    //折叠操作，aggregate的简化版操作
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val foldResult: Int = rdd.fold(0)(_+_)

    println(foldResult)
  }

  @Test
  def hello9():Unit={
    //统计每种key的个数
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    // 统计每种key的个数
    val result: collection.Map[Int, Long] = rdd.countByKey()
    println(result.mkString(","))
  }

  @Test
  def hello10():Unit={
    //将数据保存到不同格式的文件中

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
    //分布式遍历RDD中的每一个元素，调用指定函数
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 收集后打印
    rdd.map(num=>num).collect().foreach(println)

    println("****************")

    // 分布式打印
    rdd.foreach(println)
  }

}
