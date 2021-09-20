package cn.sysu.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author : song bei chang
  * @create 2021/5/3 14:41
  *         先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
  *         (1) 分别统计每个品类点击的次数，下单的次数和支付的次数：
  *                 （品类，点击总数）（品类，下单总数）（品类，支付总数）
  *         (2) 一次性统计每个品类点击的次数，下单的次数和支付的次数：
  *                 （品类，（点击总数，下单总数，支付总数））
  */
object HotTop10 {

  val sc = new SparkContext(new SparkConf().setAppName("XiaoPang").setMaster("local[*]"))
  var OUT_PATH = "output"

  def main(args: Array[String]): Unit = {
    // function_mapValues()
    // methodOne()
    methodTwo()

  }

  // TODO mapValues高阶函数  对象类型泛型：Map类型。只对集合中value做操作、key保持不变。一般用于求和，分组最大，分组最小，均值。常用于分组求聚合。
  def function_mapValues(): Unit ={
    var linelist: List[(String, Int)] = List(("hello scala hello world",4),
      ("hello world",3),("hello hadoop",2),("hello hbase",1))

    val flatmapRDD: List[(String,Int)] = linelist.flatMap(t=>{
      var line:String = t._1
      //"hello scala hello world"
      val word: Array[String] = line.split(" ")
      //"hello" "scala" "hello" "world"
      val mapRDD: Array[(String, Int)] = word.map(w=>(w,t._2))
      //("hello",4),("scala",4)....
      mapRDD
    })
    val groupRDD: Map[String, List[(String, Int)]] = flatmapRDD.groupBy(t=>t._1)
    println(groupRDD.mkString(","))
    //world -> List((world,4), (world,3)),hadoop -> List((hadoop,2)),scala -> List((scala,4)),hello -> List((hello,4), (hello,4), (hello,3), (hello,2), (hello,1)),hbase -> List((hbase,1))

    val stringToint: Map[String, Int] = groupRDD.mapValues(data=>data.map(tt=>tt._2).sum)
    println(stringToint.mkString("|"))
    //world -> 7|hadoop -> 2|scala -> 4|hello -> 14|hbase -> 1
  }

  def methodOne(): Unit ={


    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // TODO 对品类进行点击的统计
    //line => （category，clickCount）
    // （品类1， 10）
    val clickRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).filter( _._1 != "-1" )

    // 将数据按照相同的Key对Value进行聚合
    val categoryIdToClickCountRDD = clickRDD.reduceByKey(_+_)


    // TODO 对品类进行下单的统计
    //（category，orderCount）
    // （品类1，品类2，品类3， 10）
    // （品类1，10）， （品类2，10）， （品类3， 10）
    val orderRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        datas(8)
      }
    ).filter( _ != "null" )

    // 将处理的数据进行扁平化后再进行映射处理
    val orderToOneRDD = orderRDD.flatMap{
      id => {
        val ids = id.split(",")
        ids.map( id=>(id,1) )
      }
    }
    val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)


    // TODO 对品类进行支付的统计
    //（category，payCount）
    val payRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        datas(10)
      }
    ).filter( _ != "null" )

    val payToOneRDD = payRDD.flatMap{
      id => {
        val ids = id.split(",")
        ids.map( id=>(id,1) )
      }
    }
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

    // TODO 将上面统计的结果转换结构
    // 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
    // tuple => ( 元素1， 元素2， 元素3 )
    // （品类，点击数量），（品类，下单数量），（品类，支付数量）
    // ( 品类， （点击数量，下单数量，支付数量） )
    val joinRDD: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join( categoryIdToOrderCountRDD )
    val joinRDD1: RDD[(String, ((Int, Int), Int))] = joinRDD.join( categoryIdToPayCountRDD )
    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD1.mapValues {
      case ((clickCount, orderCount), payCount) => {
        (clickCount, orderCount, payCount)
      }
    }
    println("tuple:"+ mapRDD.collect().mkString(","))
    //tuple:(4,(5961,1760,1271)),(8,(5974,1736,1238)),(20,(6098,1776,1244)),(19,(6044,1722,1158)),(15,(6120,1672,1259)),(6,(5912,1768,1197)),(2,(6119,1767,1196)),(17,(6079,1752,1231)),(13,(6036,1781,1161)),(11,(6093,1781,1202)),(14,(5964,1773,1171)),(7,(6074,1796,1252)),(5,(6011,1820,1132)),(18,(6024,1754,1197)),(16,(5928,1782,1233)),(9,(6045,1736,1230)),(3,(5975,1749,1192)),(12,(6095,1740,1218)),(1,(5976,1766,1191)),(10,(5991,1757,1174))

    // TODO 将转换结构后的数据进行排序（降序）
    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, false)

    // TODO 将排序后的结果取前10名
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)
    println(result.mkString(","))
    //(15,(6120,1672,1259)),(2,(6119,1767,1196)),(20,(6098,1776,1244)),(12,(6095,1740,1218)),(11,(6093,1781,1202)),(17,(6079,1752,1231)),(7,(6074,1796,1252)),(9,(6045,1736,1230)),(19,(6044,1722,1158)),(13,(6036,1781,1161))
    result


  }

  def methodTwo(): Unit ={

    // TODO 读取电商日志数据
    val actionRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // TODO 对品类进行点击的统计
    //line =>
    //    click = (1, 0, 0)
    //    order = (0, 1, 0)
    //    pay   = (0, 0, 1)

    val flatMapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if ( datas(6) != "-1" ) {
          // 点击的场合
          List( (datas(6), (1,0,0)) )
        } else if ( datas(8) != "null" ) {
          // 下单的场合
          val ids = datas(8).split(",")
          ids.map(id=>(id, (0,1,0)))
        } else if ( datas(10) != "null" ) {
          // 支付的场合
          val ids = datas(10).split(",")
          ids.map(id=>(id, (0,0,1)))
        } else {
          Nil
        }
      }
    )
    println("flatMapRDD: "+flatMapRDD.collect().mkString(","))
    flatMapRDD.saveAsTextFile(OUT_PATH)
    // (11,(0,0,1)),(8,(0,0,1)),(6,(0,0,1)),(20,(0,1,0))

    val reduceRDD = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //reduceRDD.sortByKey()
    val result: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)

    println(result.mkString(","))

  }

  def methodThree(): Unit ={
//    // TODO 读取电商日志数据
//    val actionRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
//
//    // TODO 对品类进行点击的统计
//    // TODO 使用累加器对数据进行聚合
//    val acc = new HotCategoryAccumulator
//    EnvUtil.getEnv().register(acc, "hotCategory")
//
//    // TODO 将数据循环，向累加器中放
//    actionRDD.foreach(
//      action => {
//        val datas = action.split("_")
//        if ( datas(6) != "-1" ) {
//          // 点击的场合
//          acc.add(( datas(6), "click" ))
//        } else if ( datas(8) != "null" ) {
//          val ids = datas(8).split(",")
//          ids.foreach(
//            id => {
//              acc.add((id, "order"))
//            }
//          )
//        } else if ( datas(10) != "null" ) {
//          val ids = datas(10).split(",")
//          ids.foreach(
//            id => {
//              acc.add((id, "pay"))
//            }
//          )
//        } else {
//          Nil
//        }
//      }
//    )
//
//    // TODO 获取累加器的值
//    val accValue: mutable.Map[String, UserVisitAction] = acc.value
//    val categories: mutable.Iterable[UserVisitAction] = accValue.map(_._2)
//
//    categories.toList.sortWith(
//      (leftHC, rightHC) => {
//        if ( leftHC.clickCount > rightHC.clickCount ) {
//          true
//        } else if ( leftHC.clickCount == rightHC.clickCount ) {
//          if (leftHC.orderCount > rightHC.orderCount) {
//            true
//          } else if (leftHC.orderCount == rightHC.orderCount) {
//            leftHC.payCount > rightHC.payCount
//          } else {
//            false
//          }
//        } else {
//          false
//        }
//      }
//    ).take(10)
//
//    // 。。。
//    /**
//      * 热门品类累加器
//      * 1. 继承AccumulatorV2，定义泛型【In, Out】
//      *    IN : (品类，行为类型)
//      *    OUT : Map[品类，HotCategory]
//      * 2. 重写方法(6)
//      */
//    class HotCategoryAccumulator extends AccumulatorV2[ (String, String),mutable.Map[String, UserVisitAction]]{
//
//      val hotCategoryMap = mutable.Map[String, UserVisitAction]()
//
//      override def isZero: Boolean = hotCategoryMap.isEmpty
//
//      override def copy(): AccumulatorV2[(String, String), mutable.Map[String, UserVisitAction]] = {
//        new HotCategoryAccumulator
//      }
//
//      override def reset(): Unit = {
//        hotCategoryMap.clear()
//      }
//
//      override def add(v: (String, String)): Unit = {
//        val cid = v._1
//        val actionType = v._2
//
//        val hotCategory = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//
//        actionType match {
//          case "click" => hotCategory.clickCount += 1
//          case "order" => hotCategory.orderCount += 1
//          case "pay"   => hotCategory.payCount += 1
//          case _ =>
//        }
//        hotCategoryMap(cid) = hotCategory
//
//      }
//
//      override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, UserVisitAction]]): Unit = {
//        other.value.foreach{
//          case ( cid, hotCategory ) => {
//            val hc = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//
//            hc.clickCount += hotCategory.clickCount
//            hc.orderCount += hotCategory.orderCount
//            hc.payCount += hotCategory.payCount
//
//            hotCategoryMap(cid) = hc
//          }
//        }
//      }
//
//      override def value: mutable.Map[String, UserVisitAction] = hotCategoryMap
//    }
  }


}
