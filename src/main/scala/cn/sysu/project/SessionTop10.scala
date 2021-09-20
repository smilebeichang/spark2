package cn.sysu.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * @Author : song bei chang
  * @create 2021/5/3 17:07
  */
object SessionTop10 {

  val sc = new SparkContext(new SparkConf().setAppName("XiaoPang").setMaster("local[*]"))

  def main(args: Array[String]): Unit = {
    //analysis()
  }



  /**
    * 数据分析
    * @return
    */
  /*
  override def analysis( data : Any )  = {

    val top10: List[UserVisitAction] = data.asInstanceOf[List[UserVisitAction]]

    val top10Ids: List[String] = top10.map(_.order_category_ids)

    // TODO 使用广播变量实现数据的传播
    val bcList: Broadcast[List[String]] = EnvUtil.getEnv().broadcast(top10Ids)

    // TODO 获取用户行为的数据
    val actionRDD: RDD[UserVisitAction] = sc.getUserVisitAction("input/user_visit_action.txt")

    // TODO 对数据进行过滤
    // 对用户的点击行为进行过滤
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          bcList.value.contains(action.click_category_id.toString)
          //                    var flg = false
          //
          //                    top10.foreach(
          //                        hc => {
          //                            if (hc.categoryId.toLong == action.click_category_id) {
          //                                flg = true
          //                            }
          //                        }
          //                    )
          //
          //                    flg
        } else {
          false
        }
      }
    )

    // TODO 将过滤后的数据进行处理
    // （ 品类_session,1）=> (品类_session, sum)
    val rdd: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )

    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    // TODO 将统计后的结果进行结构的转换
    // (品类_session, sum) => (品类, (session, sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }

    // TODO 将转换结构后的数据对品类进行分组
    // (品类, Iterator[(session1, sum1), (session2, sum2)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // TODO 将分组后的数据进行排序取前10名
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(10)
      }
    )
    println(reduceRDD.collect().mkString(","))
    resultRDD.collect()

  }
*/

}
