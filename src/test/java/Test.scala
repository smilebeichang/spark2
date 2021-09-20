/**
  * @Author : song bei chang
  * @create 2021/5/31 22:26
  */
import java.text.SimpleDateFormat

import org.junit

class Test {


  @junit.Test
  def  time():Unit={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: Long = format.parse("2020-09-10 13:15:00").getTime / 1000
    println(time)

  }


}
