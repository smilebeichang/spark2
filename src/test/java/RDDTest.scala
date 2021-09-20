

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
  * @Author : song bei chang
  * @create 2021/3/15 21:13
  */

class RDDTest {

  var sc = new SparkContext(new SparkConf().setAppName("MySpark").setMaster("local[*]"))
  var OUTPATH = "output"

  @Before
  def init: Unit ={
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(OUTPATH)

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
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)
    rdd.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello2():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)
    val rdd1: RDD[Int] = rdd.map(x => x+1)
    rdd1.saveAsTextFile(OUTPATH )
  }

  @Test
  def hello3():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)
    val rdd1: RDD[Int] = rdd.map(x =>{
      println(x + "第一次map操作")
      x
    })
    val rdd2: RDD[Int] = rdd1.map(x =>{
      println(x + "第二次map操作")
      x
    })
    val rdd3 = rdd2.groupBy(x =>{
      println(x + "第三次GroupBy操作")
      x
    })
    rdd3.saveAsTextFile(OUTPATH)

  }


  @Test
  def hello4():Unit={
    val rdd: RDD[String] = sc.textFile("input/apache.log",1)
    val result: RDD[String] = rdd.map(line => line.split(" ")(6))
    result.saveAsTextFile(OUTPATH)
  }


  @Test
  def hello5():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)

    //val result: RDD[Int] = rdd.mapPartitions(x => x.map(ele => ele * 2 ).toIterator)
    val result: RDD[Int] = rdd.mapPartitions(x => x.filter(ele => ele % 2 == 1))

    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello6():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)


    val result: RDD[Int] = rdd.mapPartitions(it => List(it.max).iterator)

    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello7():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)


   val result: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,iter) => iter.map(elem => (index,elem)))

    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello8():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)


    val result = rdd.mapPartitionsWithIndex{
      case (index,iter) if(index == 1 ) => iter
      case _ => Nil.iterator
    }

    result.saveAsTextFile(OUTPATH)
  }


  @Test
  def hello9():Unit={
    val list = List(List(1,2,3),4,List(5,6,7,8,9,10))
    val rdd: RDD[Any] = sc.makeRDD(list,2)

    val result: RDD[Any] = rdd.flatMap {
      case x: Int => List(x)
      case y: List[Any] => y
    }
    result.saveAsTextFile(OUTPATH)

  }


  @Test
  def hello10():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Any] = sc.makeRDD(list,2)

    val result: RDD[Array[Any]] = rdd.glom()
    result.saveAsTextFile(OUTPATH)
    result.collect().foreach(x => println(x.mkString(",")))

  }


  @Test
  def hello11():Unit={
    val list = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)
    val result: RDD[Int] = rdd.filter(x => x % 2 == 1)
    println(result)
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello12():Unit={
    val rdd: RDD[String] = sc.textFile("input/apache.log",1)
    val add2: RDD[String] = rdd.filter(line => line.split(" ")(3).contains("18/05/2015"))
    val result: RDD[String] = add2.map(line => line.split(" ")(3))
    result.saveAsTextFile(OUTPATH)
    println(result)
  }
  
  @Test
  def hello13():Unit={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),1)
    val result: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello14():Unit={
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"),2)
    val result: RDD[(Char, Iterable[String])] = rdd.groupBy(word => word(0))
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello15():Unit={
    val fileRDD: RDD[String] = sc.textFile("input/apache.log")

    val timeRDD = fileRDD.map(
      log => {
        val datas = log.split(" ")
        datas(3)
      }
    )

    val hourRDD: RDD[(String, Iterable[String])] = timeRDD.groupBy(
      time => {
        time.substring(11, 13)
      }
    )
//    hourRDD.saveAsTextFile(OUTPATH)

    val result: RDD[(String, Iterable[String])] = fileRDD.groupBy(log => {log.split(" ")(3).substring(11,13)})
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello16():Unit={
    //(Hello,2),(Scala,1)
    val rdd = sc.makeRDD(List("Hello Scala", "Hello"))

    println(rdd
      .flatMap(_.split(" "))
      .groupBy(word => word)
      .map(kv => (kv._1, kv._2.size))
      .collect().mkString(","))
  }

  @Test
  def hello17():Unit={
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),1)
//    val result: RDD[Int] = rdd.sample(false,0.2)
    val result: RDD[Int] = rdd.sample(true,2)
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello18():Unit={
    val rdd = sc.makeRDD(List(1,2,3,4,5,1,2),1)
//    val result: RDD[Int] = rdd.distinct()
    val result: RDD[Int] = rdd.distinct(2)
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello19():Unit={
    val rdd = sc.makeRDD(List(
      1,2,3,4,1,2
    ),6)

    val result = rdd.coalesce(2)
    result.saveAsTextFile(OUTPATH)

  }

  @Test
  def hello20():Unit={
    val rdd = sc.makeRDD(List(
      1,2,3,4,1,2
    ),2)

    val result = rdd.repartition(4)
    result.saveAsTextFile(OUTPATH)
  }

  @Test
  def hello21():Unit={
    val rdd = sc.makeRDD(List(
      1,2,3,4,1,2
    ),2)

    val result = rdd.sortBy(num=>num, false, 2)
    result.saveAsTextFile(OUTPATH)


  }
}

