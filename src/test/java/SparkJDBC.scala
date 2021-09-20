import java.util.{Properties, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.junit

import scala.util.Random

/**
  * @Author : song bei chang
  * @create 2021/3/20 18:38
  */
class SparkJDBC {


  @junit.Test
  def hello1():Unit={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    println("======= 方式1：通用的load方法读取 =======")
    //方式1：通用的load方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://ecs2:3306/hive")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "sbc006688")
      .option("dbtable", "iqc_conversation_info")
      .option("useSSL", "false")
      .load().show


    println("======= 方式2:通用的load方法读取 参数Map形式 =======")
    //方式2:通用的load方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://localhost:3306/java130?user=root&password=root",
        "dbtable"->"user","driver"->"com.mysql.jdbc.Driver","useSSL"->"true")).load().show


    println("======= 方式3:使用jdbc方法读取 =======")
    //方式3:使用jdbc方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    props.setProperty("useSSL", "false")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/java130", "user", props)
    df.show

    //释放资源
    spark.stop()
  }



  @Test
  def hello2():Unit={

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2(Random.nextInt(),"bao", "bao","1","bao"), User2(Random.nextInt(),"pang",  "pang","1","pang")))
    val ds: Dataset[User2] = rdd.toDS
    //方式1：通用的方式  format指定写出类型
   ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/java130?useSSL=false")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    val rdd1: RDD[User2] = spark.sparkContext.makeRDD(List(User2(Random.nextInt(),"bao", "bao","1","bao"), User2(Random.nextInt(),"pang",  "pang","1","pang")))
    val ds1: Dataset[User2] = rdd1.toDS
    //方式2：通过jdbc方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    ds1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/java130?useSSL=false", "user", props)

    Thread.sleep(100000)
    //释放资源
    spark.stop()

  }

}

case class User2(uid:Long,userName: String, password: String,status:String,salt: String)
