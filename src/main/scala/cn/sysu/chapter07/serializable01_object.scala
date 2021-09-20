package cn.sysu.chapter07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/6/1 21:13
  */
object serializable01_object {


  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[4]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建两个对象
    val user1 = new User()
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"

    val userRDD1: RDD[User] = sc.makeRDD(List(user1, user2))

    //3.1 打印，ERROR报java.io.NotSerializableException
    //userRDD1.foreach(user => println(user.name))


    //3.2 打印，RIGHT （因为没有传对象到Executor端）
    val userRDD2: RDD[User] = sc.makeRDD(List())
    //userRDD2.foreach(user => println(user.name))

    //3.3 打印，ERROR Task not serializable 注意：没执行就报错了
    userRDD2.foreach(user => println(user1.name))

    //4.关闭连接
    sc.stop()
  }
}

//class User {
//  var name: String = _
//}
class User extends Serializable {
  var name: String = _
}