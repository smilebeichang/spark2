package cn.sysu.project

/**
  * @Author : song bei chang
  * @create 2021/3/18 23:30
  */
object WordCountApp extends BaseApp {

  override val OUT_PATH :String = "outputProject/outputProject3"

  def main(args: Array[String]): Unit = {

    run{
      sc.makeRDD(List(1,2,3,4),2).saveAsTextFile(OUT_PATH)
    }

  }



}
