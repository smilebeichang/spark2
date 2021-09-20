package cn.sysu.homework

import java.text.SimpleDateFormat
import java.util.UUID

/**
  * @Author : song bei chang
  * @create 2021/5/31 22:19
  */
object UserBehaviorTrajectory {


  case class User(var uid: String, var id: String, var time: Long, var link: String)

  def main(args: Array[String]): Unit = {

    val list = List[(String, String, String)](
      ("1001", "2020-09-10 10:21:21", "home.html"),
      ("1001", "2020-09-10 10:28:10", "good_list.html"),
      ("1001", "2020-09-10 10:35:05", "good_detail.html"),
      ("1001", "2020-09-10 10:42:55", "cart.html"),
      ("1001", "2020-09-10 11:35:21", "home.html"),
      ("1001", "2020-09-10 11:36:10", "cart.html"),
      ("1001", "2020-09-10 11:38:12", "trade.html"),
      ("1001", "2020-09-10 11:40:00", "payment.html"),
      ("1002", "2020-09-10 09:40:00", "home.html"),
      ("1002", "2020-09-10 09:41:00", "mine.html"),
      ("1002", "2020-09-10 09:42:00", "favor.html"),
      ("1003", "2020-09-10 13:10:00", "home.html"),
      ("1003", "2020-09-10 13:15:00", "search.html")
    )

    // 处理时间,封装对象
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val res = list.map {
      case (id, time, link) => User(UUID.randomUUID().toString, id, format.parse(time).getTime / 1000, link)
    }
      //分组
      .groupBy(_.id).map(_._2)

      //此时数据为
      //List(User(b53,1003,1599714600,home.html), User(f41,1003,1599714900,search.html))
      .flatMap(x => {
      val slidingList = x.sliding(2)
      //两两滑窗,再判断时间
      slidingList.foreach(y => {
        //此时数据为
        //List(User(aa0,1001,1599704481,home.html), User(297,1001,1599704890,good_list.html))
        //List(User(97,1001,1599704890,good_list.html), User(874,1001,1599705305,good_detail.html))
        val head = y.head
        val last = y.last

        //如果前后时间不相差超过三十分钟,就把前面的uid赋值给后面的
        //结果如下
        //List(User(aa0,1001,1599704481,home.html), User(aa0,1001,1599704890,good_list.html))
        //List(User(aa0,1001,1599704890,good_list.html), User(874,1001,1599705305,good_detail.html))

        if (last.time - head.time <= 60 * 30) last.uid = head.uid
      })
      //返回x很重要，保证数据内存地址变化后，实时更新数据集
      x
    })
      //此时数据为
      //User(e8ed4922-6be0-4ef7-84f3-ed653f267fd9,1003,1599714600,home.html)
      //User(e8ed4922-6be0-4ef7-84f3-ed653f267fd9,1003,1599714900,search.html)
      //User(bdb8a4ee-b360-45fd-9431-04585807a510,1002,1599702000,home.html)
      //再按照uid分组,与下标拉链
      .groupBy(_.uid)
      .map(z => {
        val zipList = z._2.zipWithIndex
        //下标加一
        zipList.map {
          case (user, index) => (user, index + 1)
        }
      })
      //List((User(bcd,1003,1599714600,home.html),1), (User(bcd,1003,1599714900,search.html),2))
      //再按照id和时间排序,最后拉平得到结果
      .toList.sortWith((a, b) => {
      if (a.head._1.id > b.head._1.id) false
      else if (a.head._1.id < b.head._1.id) true
      else {
        if (a.head._1.time > b.head._1.time) false
        else true
      }
    }).flatten

    res.foreach(println(_))
    //(User(d76e2807-5540-4ef6-98e2-648ac0cf69af,1001,1599704481,home.html),1)
    //(User(d76e2807-5540-4ef6-98e2-648ac0cf69af,1001,1599704890,good_list.html),2)
    //(User(d76e2807-5540-4ef6-98e2-648ac0cf69af,1001,1599705305,good_detail.html),3)
    //(User(d76e2807-5540-4ef6-98e2-648ac0cf69af,1001,1599705775,cart.html),4)
    //(User(9ce84306-2ffb-4c3a-a7a0-e9dc2b0310f6,1001,1599708921,home.html),1)
    //(User(9ce84306-2ffb-4c3a-a7a0-e9dc2b0310f6,1001,1599708970,cart.html),2)
    //(User(9ce84306-2ffb-4c3a-a7a0-e9dc2b0310f6,1001,1599709092,trade.html),3)
    //(User(9ce84306-2ffb-4c3a-a7a0-e9dc2b0310f6,1001,1599709200,payment.html),4)
    //(User(0c997a09-63b9-4407-9756-f1319f6e4e4a,1002,1599702000,home.html),1)
    //(User(0c997a09-63b9-4407-9756-f1319f6e4e4a,1002,1599702060,mine.html),2)
    //(User(0c997a09-63b9-4407-9756-f1319f6e4e4a,1002,1599702120,favor.html),3)
    //(User(d8aade40-50f7-4eba-bd58-c11100470977,1003,1599714600,home.html),1)
    //(User(d8aade40-50f7-4eba-bd58-c11100470977,1003,1599714900,search.html),2)
  }




}
