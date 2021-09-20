package cn.sysu.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit

/**
  * @Author : song bei chang
  * @create 2021/3/20 17:25
  */
class SparkSQL {


  @junit.Test
  def hello1():Unit={

    //创建上下文环境配置对象  浏览器的页签标题
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("xiaopang Spark")

    //创建SparkSession对象
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名 这里的spark对象不能使用var声明，因为Scala只支持val修饰的对象的引入。
    import session.implicits._

    //读取json文件 创建DataFrame  {"username": "lisi","age": 18}
    val df: DataFrame = session.read.json("input/person.json")
    df.show()

    //SQL风格语法
    df.createOrReplaceTempView("person")
    session.sql("select avg(age) from person").show

    //DSL风格语法
    df.select("username","age").show()
    df.agg("age"->"avg").show()

    //*****RDD=>DataFrame=>DataSet*****

    //RDD
    val rdd1: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))

    println("==========DataFrame=============")
    //DataFrame
    val df1: DataFrame = rdd1.toDF("id","name","age")
    df1.show()

    println("==========DateSet=============")
    //DateSet
    val ds1: Dataset[User] = df1.as[User]
    ds1.show()


    //*****DataSet=>DataFrame=>RDD*****
    //DataFrame
    val df2: DataFrame = ds1.toDF()

    //RDD  返回的RDD类型为Row，里面提供的getXXX方法可以获取字段值，类似jdbc处理结果集，但是索引从0开始
    val rdd2: RDD[Row] = df2.rdd
    rdd2.foreach(a=>println(a.getString(1)))

    //*****RDD=>DataSet*****
    rdd1.map{
      case (id,name,age)=>User(id,name,age)
    }.toDS()

    //*****DataSet=>=>RDD*****
    ds1.rdd

    Thread.sleep(1000000)
    //释放资源
    session.stop()

  }


}

case class User(id:Int,name:String,age:Int)
