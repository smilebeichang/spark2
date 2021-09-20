import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
  * @Author : song bei chang
  * @create 2021/3/16 19:54
  *        双value
  */
class RDD2Test {
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
    val list1 = List(1,2,3,4)
    val list2 = List(3,4,5,6)
    val list3 = List("xiao"," shuai","xiao"," pang")
    val rdd1: RDD[Int] = sc.makeRDD(list1,2)
    val rdd2: RDD[Int] = sc.makeRDD(list2,2)
    val rdd3: RDD[String] = sc.makeRDD(list3,2)
//    rdd1.intersection(rdd2).saveAsTextFile(OUT_PATH)
//    rdd1.intersection(rdd3).saveAsTextFile(OUT_PATH)
//    rdd1.union(rdd2).saveAsTextFile(OUT_PATH)
//    rdd1.subtract(rdd2).saveAsTextFile(OUT_PATH)
    rdd1.zip(rdd3).saveAsTextFile(OUT_PATH)
  }

  @Test
  def hello2():Unit={
    val rdd: RDD[(Int, String)] =
      sc.makeRDD(Array((1,"aaa"),(1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    import org.apache.spark.HashPartitioner
    val rdd2: RDD[(Int, String)] =
      rdd.partitionBy(new HashPartitioner(2))

    rdd2.saveAsTextFile(OUT_PATH)
  }
  
  @Test
  def hello3():Unit={
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("c",3)))
    val rdd2 = rdd1.reduceByKey(_+_)
    val rdd3 = rdd1.reduceByKey(_+_, 2)
    rdd3.saveAsTextFile(OUT_PATH)
    
  }

  @Test
  def hello4():Unit={
    val list = List((1,1),(2,1),(3,1),(1,1),(2,2),(3,2),(1,1),(2,1),(3,1),(1,1),(2,2),(3,2))
    sc.makeRDD(list).reduceByKey(_ + _ ).collect()
    sc.makeRDD(list).groupByKey().collect()

  }

  @Test
  def hello5():Unit={
    val rdd1 =
      sc.makeRDD(List((1,1),(2,2),(2,3)))
    val rdd2 =
      rdd1.aggregateByKey(0)(_+_,_+_)
    rdd2.saveAsTextFile(OUT_PATH)

  }

  @Test
  def hello6():Unit={
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd2 = rdd1.foldByKey(0)(_+_)
    rdd2.saveAsTextFile(OUT_PATH)

  }

  @Test
  def hello7():Unit={
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val srdd2: RDD[(String, Int)] = rdd1.sortByKey(true,2)
    val srdd3: RDD[(String, Int)] = rdd1.sortByKey(false)

    srdd3.saveAsTextFile(OUT_PATH)

  }

  @Test
  def hello8():Unit={
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))
    rdd.join(rdd1).collect().foreach(println)
  }

  @Test
  def hello9():Unit={

    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("b",2)))

    dataRDD1.leftOuterJoin(dataRDD2).collect().foreach(println)

  }

  @Test
  def hello10():Unit={
    val dataRDD1 = sc.makeRDD(List(("a",1),("a",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("c",2),("c",3)))

    dataRDD1.cogroup(dataRDD2).collect().foreach(println)

  }
}
