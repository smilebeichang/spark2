import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
  * @Author : song bei chang
  * @create 2021/3/18 12:48
  */
class Accumulator {

  var sc = new SparkContext(new SparkConf().setAppName("xiao_pang_Spark").setMaster("local[*]"))
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
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    // 声明累加器
    var sum = sc.longAccumulator("sum");
    rdd.foreach(
      num => {
        // 使用累加器
        sum.add(num)
      }
    )
    // 获取累加器的值
    println("sum = " + sum.value)
  }
  
  @Test
  def hello2():Unit={
    val rdd1 = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ),4)
    val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )
    // 声明广播变量
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (key, num) => {
        var num2 = 0
        // 使用广播变量
        for ((k, v) <- broadcast.value) {
          if (k == key) {
            num2 = v
          }
        }
        (key, (num, num2))
      }
    }
    resultRDD.saveAsTextFile(OUT_PATH)
  }
}
