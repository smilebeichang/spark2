package cn.sysu.chapter07

import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}

case class UserAnalysis(userid:String,time:Long,page:String,var session:String=UUID.randomUUID().toString,var step:Int=1)
object Test {

  val i:Int = 3;

  def main(args: Array[String]): Unit = {

    val list = List[(String,String,String)](
      ("1001","2020-09-10 10:21:21","home.html"),
      ("1001","2020-09-10 10:28:10","good_list.html"),
      ("1001","2020-09-10 10:35:05","good_detail.html"),
      ("1001","2020-09-10 10:42:55","cart.html"),
      ("1001","2020-09-10 11:35:21","home.html"),
      ("1001","2020-09-10 11:36:10","cart.html"),
      ("1001","2020-09-10 11:38:12","trade.html"),
      ("1001","2020-09-10 11:40:00","payment.html"),
      ("1002","2020-09-10 09:40:00","home.html"),
      ("1002","2020-09-10 09:41:00","mine.html"),
      ("1002","2020-09-10 09:42:00","favor.html"),
      ("1003","2020-09-10 13:10:00","home.html"),
      ("1003","2020-09-10 13:15:00","search.html")
    )

    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    val rdd = sc.parallelize(list)

    //1、转换数据类型
    val rdd2 = rdd.map{
      case (userid,timestr,page) =>
        val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = formatter.parse(timestr).getTime
        UserAnalysis(userid,time,page)
    }
    //UserAnalysis(1001,1599709092000,trade.html,b61db323-9337-406c-b0ff-ce5e1bfa1e5f,1)
    //UserAnalysis(1001,1599705775000,cart.html,c15f6794-4707-4ebb-a547-708e7ff1cae4,1)
    //UserAnalysis(1002,1599702060000,mine.html,e4b5036c-b7ee-459e-b26a-fec52730b86d,1)
    //UserAnalysis(1001,1599709200000,payment.html,8f228b1b-ae2f-4aef-b7e6-907b7aecfa25,1)
    //UserAnalysis(1001,1599704481000,home.html,dc3bb607-483d-43f8-b123-24a9fcdc574d,1)
    //UserAnalysis(1002,1599702120000,favor.html,93f609ab-d540-473e-8bec-5903202c7a66,1)
    //UserAnalysis(1001,1599708921000,home.html,69999169-8fb9-49b8-93de-63ed5818726b,1)
    //UserAnalysis(1002,1599702000000,home.html,a575ee59-a4ed-4e4d-99a0-53d5222bff2e,1)
    //UserAnalysis(1003,1599714600000,home.html,4c032694-0c69-43fe-a911-7f56c730a119,1)
    //UserAnalysis(1001,1599708970000,cart.html,449518b1-494c-407f-98e3-15c07b419025,1)
    //UserAnalysis(1003,1599714900000,search.html,898b6c9a-eff8-4116-83e2-27fab10de018,1)
    //UserAnalysis(1001,1599704890000,good_list.html,9e0908f4-49f7-417e-82a5-30c3a38b7676,1)
    //UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1)
    //2、按照用户分组
    val rdd3 = rdd2.groupBy(x=>x.userid)
    //[
    //    1001-> List(
    //              UserAnalysis(1001,1599704890000,good_list.html,9e0908f4-49f7-417e-82a5-30c3a38b7676,1)
    //              UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1)
    //              UserAnalysis(1001,1599708970000,cart.html,449518b1-494c-407f-98e3-15c07b419025,1)
    //              UserAnalysis(1001,1599708921000,home.html,69999169-8fb9-49b8-93de-63ed5818726b,1)
    //              UserAnalysis(1001,1599709092000,trade.html,b61db323-9337-406c-b0ff-ce5e1bfa1e5f,1)
    //              UserAnalysis(1001,1599705775000,cart.html,c15f6794-4707-4ebb-a547-708e7ff1cae4,1)
    //              ....
    //         )
    //    1002->
    //    1003->
    // ]
    //3、对每个用户的所有数据进行排序
    val rdd4 = rdd3.flatMap(x=>{

      //    1001-> List(
      //              UserAnalysis(1001,1599704890000,good_list.html,9e0908f4-49f7-417e-82a5-30c3a38b7676,1)
      //              UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1)
      //              UserAnalysis(1001,1599708970000,cart.html,449518b1-494c-407f-98e3-15c07b419025,1)
      //              UserAnalysis(1001,1599708921000,home.html,69999169-8fb9-49b8-93de-63ed5818726b,1)
      //              UserAnalysis(1001,1599709092000,trade.html,b61db323-9337-406c-b0ff-ce5e1bfa1e5f,1)
      //              UserAnalysis(1001,1599705775000,cart.html,c15f6794-4707-4ebb-a547-708e7ff1cae4,1)
      //              ....
      //         )
      val sortList = x._2.toList.sortBy(_.time)

      val slidingList = sortList.sliding(2)

      //List(
      //              List ( UserAnalysis(1001,1599704890000,good_list.html,9e0908f4-49f7-417e-82a5-30c3a38b7676,1),UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1))
      //              List( UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1),UserAnalysis(1001,1599705775000,cart.html,c15f6794-4707-4ebb-a547-708e7ff1cae4,1))
      //              List( UserAnalysis(1001,1599705775000,cart.html,c15f6794-4707-4ebb-a547-708e7ff1cae4,1),UserAnalysis(1001,1599708921000,home.html,69999169-8fb9-49b8-93de-63ed5818726b,1))
      //              ....
      //         )
    //4、两两比较,是否属于同一次会话【如果属于同一次会话,修改sessionid与step】
      slidingList.foreach(windown => {
        //windown = List ( UserAnalysis(1001,1599704890000,good_list.html,9e0908f4-49f7-417e-82a5-30c3a38b7676,1),UserAnalysis(1001,1599705305000,good_detail.html,87ccc53e-584b-43b3-8149-045d271469fa,1))
        val first = windown.head
        val next = windown.last
        if(next.time-first.time<=30*60*1000){
          next.session = first.session
          next.step = first.step+1
        }
      })

      x._2
    })

    //5、结果展示
    rdd4.foreach(println(_))
  }
}
