package cn.sysu.table_sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.{After, Before}

/**
  * @Author : song bei chang
  * @create 2021/5/2 15:17
  *        RDD转换算子     Value类型、双Value类型和Key-Value类型
  */
class Transformation {

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

  /* ==============  Value 类型  ================== */
  // def map[U: ClassTag](f: T => U): RDD[U]
  @Test
  def hello1():Unit={
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd1: RDD[Int] = dataRDD.map(
      num => {
        num * 2
      }
    )
    val rdd2: RDD[String] = dataRDD.map(
      num => {
        "" + num
      }
    )
    rdd1.collect().foreach(print)
    println("-^"*10)
    rdd2.collect().foreach(print)
  }

  @Test
  def hello2():Unit={

    val fileRDD = sc.textFile("input/apache.log")

    //执行时无序，最终输出有序  有趣
    val urlRDD: RDD[String] = fileRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )
    urlRDD.collect().foreach(println)
  }

  //def mapPartitions[U: ClassTag](
  //      f: Iterator[T] => Iterator[U],
  //      preservesPartitioning: Boolean = false): RDD[U]
  @Test
  def hello3():Unit={
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val dataRDD1: RDD[Int] = dataRDD.mapPartitions(
      datas => {
        datas.filter(_%2==0)
      }
    )
    dataRDD1.collect().foreach(println)
  }

  @Test
  def hello4():Unit={
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5),2)
    // 获取每个数据分区的最大值  //3,6  分区规则是什么？1,2,3  5,6
    val rdd: RDD[Int] = dataRDD.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    println(rdd.collect().mkString(","))
  }

  //def mapPartitionsWithIndex[U: ClassTag](
  //      f: (Int, Iterator[T]) => Iterator[U],
  //      preservesPartitioning: Boolean = false): RDD[U]
  @Test
  def hello5():Unit={
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5),2)
    // 获取第二个数据分区的数据，获取的分区索引从0开始
    val rdd = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if ( index == 1 ) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    println(rdd.collect().mkString(","))
  }

  //def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
  @Test
  def hello6():Unit={
    val dataRDD = sc.makeRDD(List(
      List(1,2),List(3,4)
    ),1)
    val dataRDD1 = dataRDD.flatMap(
      list => list
    )
    println(dataRDD1.collect().mkString(","))
  }

  @Test
  def hello7():Unit={
    val dataRDD = sc.makeRDD(
      List(List(1,2),3,List(4,5))
    )

    //match 匹配
    val rdd = dataRDD.flatMap(
      data => {
        data match {
          case list: List[_] =>list
          case d => List(d)
        }
      }
    )
    println(rdd.collect().mkString(","))
  }

  //def glom(): RDD[Array[T]]
  @Test
  def hello8():Unit={
    val dataRDD = sc.makeRDD(List(
      1,2,3,4
    ),1)
    val rddArray:RDD[Array[Int]] = dataRDD.glom()
    println("内存地址： "+ rddArray.collect().mkString(","))
    //System.out.println(util.Arrays.toString(rddArray))
    //rddArray.collect()(0).length
    println(rddArray.collect()(0).mkString(","))

  }

  @Test
  def hello9():Unit={
    //小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)

    // 将每个分区的数据转换为数组
    val glomRDD: RDD[Array[Int]] = dataRDD.glom()

    // 将数组中的最大值取出
    // Array => max
    val maxRDD: RDD[Int] = glomRDD.map(array=>array.max)

    // 将取出的最大值求和
    val rddArray: Array[Int] = maxRDD.collect()
    println(rddArray.mkString(","))
    println(rddArray.sum)
  }

  @Test
  def hello10():Unit={
    val dataRDD = sc.makeRDD(List(1,2,3,4),1)
    val rdd = dataRDD.groupBy(
      _%2
    )
    println(rdd)
  }

  @Test
  def hello11():Unit={
    //小功能：将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组。
    // 根据单词首写字母进行分组
    val dataRDD = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)

    val rdd: RDD[(Char, Iterable[String])] = dataRDD.groupBy(word => {
      //word.substring(0,1)
      word.charAt(0)

      //String(0) => StringOps
      // 隐式转换
      //word(0)
    })
    println(rdd)
    rdd.saveAsTextFile(OUT_PATH)
  }

  @Test
  def hello12():Unit={
    //小功能：从服务器日志数据apache.log中获取每个时间段访问量。 小时
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
    hourRDD.saveAsTextFile(OUT_PATH)
    println(hourRDD.map(kv => (kv._1,kv._2.size)).collect().mkString(","))
  }

  @Test
  def hello13():Unit={
    //小功能：WordCount。
    val dataRDD = sc.makeRDD(List("Hello Scala", "Hello"))

    println(dataRDD
      .flatMap(_.split(" "))
      .groupBy(word => word)
      .map(kv => (kv._1, kv._2.size))
      .collect().mkString(","))
  }

  @Test
  def hello14():Unit={
    //小功能：从服务器日志数据apache.log中获取2015年5月17日的请求路径
    val fileRDD: RDD[String] = sc.textFile("input/apache.log")

    val timeRDD = fileRDD.map(
      log => {
        val datas = log.split(" ")
        datas(3)
      }
    )

    val filterRDD: RDD[String] = timeRDD.filter(time => {
      val ymd = time.substring(0, 10)
      ymd == "17/05/2015"
    })
    filterRDD.collect().foreach(println)
  }

  @Test
  def hello15():Unit={
    val dataRDD = sc.makeRDD(List(
      1,2,3,4
    ),1)
    // 抽取数据不放回（伯努利算法）
    val dataRDD1 = dataRDD.sample(false, 0.5,2).collect().mkString(",")
    println(dataRDD.sample(false, 0.5, 2).collect().mkString(","))
    println(dataRDD.sample(false, 0.5, 2).collect().mkString(","))
    println(dataRDD.sample(false, 0.5, 2).collect().mkString(","))
    println(dataRDD.sample(false, 0.5, 3).collect().mkString(","))
    // 抽取数据放回（泊松算法）
    val dataRDD2 = dataRDD.sample(true, 2).collect().mkString(",")

    println(dataRDD1.toString())
    println(dataRDD2.toString())
  }

  @Test
  def hello16():Unit={
    // 将数据集中重复的数据去重
    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),1)
    val dataRDD1 = dataRDD.distinct()

    val dataRDD2 = dataRDD.distinct(2)

    dataRDD1.collect().foreach(print)
    println()
    dataRDD2.collect().foreach(print)
  }

  @Test
  def hello17():Unit={
    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),6)
    dataRDD.saveAsTextFile(OUT_PATH+"1")
    val dataRDD1 = dataRDD.coalesce(2)
    dataRDD1.saveAsTextFile(OUT_PATH)

  }


  @Test
  def hello18():Unit={
    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),2)

    val dataRDD1 = dataRDD.repartition(4)
    dataRDD1.saveAsTextFile(OUT_PATH+"2")

  }

  @Test
  def hello19():Unit={
    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),2)

    val dataRDD1 = dataRDD.sortBy(num=>num, false, 4)
    dataRDD1.saveAsTextFile(OUT_PATH+"3")
    //4,3,22,11
  }


  // ====================  双Value类型  ========================
  @Test
  def hello20():Unit={
    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
    //交集
    val dataRDD_intersection = dataRDD1.intersection(dataRDD2)
    println(dataRDD_intersection.collect().mkString(","))

    //并集
    val dataRDD_union = dataRDD1.union(dataRDD2)
    println(dataRDD_union.collect().mkString(","))

    //差集
    val dataRDD_subtract = dataRDD1.subtract(dataRDD2)
    println(dataRDD_subtract.collect().mkString(","))

    //zip
    val dataRDD_zip = dataRDD1.zip(dataRDD2)
    println(dataRDD_zip.collect().mkString(","))

  }

  // ================== key - value 类型 =================
  @Test
  def hello21():Unit={
    val rdd: RDD[(Int, String)] =
      sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    import org.apache.spark.HashPartitioner
    val rdd2: RDD[(Int, String)] =
      rdd.partitionBy(new HashPartitioner(2))

    println(rdd2.collect().mkString(","))
    rdd2.saveAsTextFile(OUT_PATH)

  }

  @Test
  def hello22():Unit={
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("c",3)))
    val dataRDD2 = dataRDD1.reduceByKey(_+_)
    val dataRDD3 = dataRDD1.reduceByKey(_+_, 2)
    dataRDD2.saveAsTextFile(OUT_PATH+"4")
    dataRDD3.saveAsTextFile(OUT_PATH+"5")

  }

  @Test
  def hello23():Unit={
    //将分区的数据直接转换为相同类型的内存数组进行后续处理
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = dataRDD1.groupByKey()
    val dataRDD3 = dataRDD1.groupByKey(2)
    val dataRDD4 = dataRDD1.groupByKey(new HashPartitioner(2))

    dataRDD2.saveAsTextFile(OUT_PATH+"4")
    dataRDD3.saveAsTextFile(OUT_PATH+"5")
    dataRDD4.saveAsTextFile(OUT_PATH+"6")

  }


  @Test
  def hello24():Unit={
    // 取出每个分区内相同key的最大值然后分区间相加
    // TODO : 取出每个分区内相同key的最大值然后分区间相加
    // 函数柯里化：将一个接收多个参数的函数转化成一个接受一个参数的函数过程，可以简单的理解为一种特殊的参数列表声明方式。
    // aggregateByKey算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    //    2.1 第一个参数表示分区内的计算规则
    //    2.2 第二个参数表示分区间的计算规则
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)
    ),2)
    // 0:("a",1),("a",2),("c",3) => (a,10)(c,10)
    //                                         => (a,10)(b,10)(c,20)
    // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)
    val resultRDD =
      rdd.aggregateByKey(10)(
        (x, y) => math.max(x,y),
        (x, y) => x + y
      )
    resultRDD.collect().foreach(println)

    // 0:("a",1),("a",2),("c",3) => (a,13)(c,13)
    //                                         => (a,13)(b,14)(c,34)
    // 1:("b",4),("c",5),("c",6) => (b,14)(c,21)
    val resultRDD2 =
      rdd.aggregateByKey(10)(
        (x, y) => x + y,
        (x, y) => x + y
      )
    resultRDD2.collect().foreach(println)

  }

  @Test
  def hello25():Unit={
    // 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
    val dataRDD1 = sc.makeRDD(List(("a",1),("a",2),("c",3), ("b",4),("c",5),("c",6)))
    val dataRDD2 = dataRDD1.foldByKey(10)(_+_)

    println(dataRDD2.collect().mkString(","))
  }

  @Test
  def hello26():Unit={
    //在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("b",3)))
    val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(true)
    val sortRDD2: RDD[(String, Int)] = dataRDD1.sortByKey(false)
    println(sortRDD1.collect().mkString(","))
    println(sortRDD2.collect().mkString(","))
  }

  @Test
  def hello27():Unit={
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"),(4,"d")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))
    rdd.join(rdd1).collect().foreach(println)
  }

  @Test
  def hello28():Unit={
    //类似于SQL语句的左外连接
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("d",4)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("b",2),("b",3),("c",3)))

    val value: RDD[(String, (Int, Option[Int]))] = dataRDD1.leftOuterJoin(dataRDD2)
    println(value.collect().mkString(","))
  }

  @Test
  def hello29():Unit={
    //在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    val dataRDD1 = sc.makeRDD(List(("a",1),("a",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("c",2),("c",3),("c",4)))

    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = dataRDD1.cogroup(dataRDD2)
    println(value.collect().mkString(","))

  }


}
