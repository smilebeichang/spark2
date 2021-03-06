package cn.sysu.day02

/**
  * @Author : song bei chang
  * @create 2021/3/24 18:01
  */
object ResuceSource {

    def main(args: Array[String]): Unit = {

      val list = List(1,2,3,4)


      // 将数据两两结合，实现运算规则
      val i: Int = list.reduce( (x,y) => x-y )
      println("i = " + i)

      // 从源码的角度，reduce底层调用的其实就是reduceLeft
      //val i1 = list.reduceLeft((x,y) => x-y)

      // ((4-3)-2-1) = -2
      val i2 = list.reduceRight((x,y) => x-y)
      println(i2)
    }

}
