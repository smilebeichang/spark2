package cn.sysu.project

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @Author : song bei chang
  * @create 2021/3/18 23:21
  */
abstract class BaseApp {

  val sc = new SparkContext(new SparkConf().setAppName("XiaoPangSpark").setMaster("local[*]"))
  val OUT_PATH:String

  def run( op: Unit )={
    init()
    try {
      println("run 運行")
      op
    } catch {
      case e: Exception => println(e)
    } finally {
      stop()
    }
  }



  def init(): Unit ={
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(OUT_PATH)

    //如果目录存在,则删除
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }


  def stop(): Unit ={
    sc.stop()
  }

}
