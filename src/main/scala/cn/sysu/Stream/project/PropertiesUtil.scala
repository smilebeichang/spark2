package cn.sysu.Stream.project

import java.io.InputStreamReader
import java.util.Properties

/**
  * @Author : song bei chang
  * @create 2021/5/21 18:06
  */
object PropertiesUtil {

  def load(propertiesName:String): Properties ={

    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) , "UTF-8"))
    prop
  }

}
