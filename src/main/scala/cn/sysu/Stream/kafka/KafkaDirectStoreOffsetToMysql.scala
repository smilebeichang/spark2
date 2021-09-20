package com.atguigu.sparksteaming.kafka

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.atguigu.sparksteaming.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.{immutable, mutable}

/**
 * Created by Smexy on 2021/7/3
 *
 *    自己维护offset!
 *
 *
 *    核心： ①取消自动提交offset
 *          ②在程序开始计算之前，先从 mysql中读取上次提交的offsets
 *          ③基于上次提交的offsets，构造一个DS，这个DS从上次提交的offsets位置向后消费的数据流
 *              def SubscribePattern[K, V](
 *                      pattern: ju.regex.Pattern,
 *                      kafkaParams: collection.Map[String, Object],
 *                     offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V]
 *
 *
 *          ④需要将结果，收集到Driver端，和offsets信息，组合为一个事务，一起写入数据库
 *                成功，提交，失败，就回滚！
 *
 *
 *
 *
 *      Mysql中表的设计：
 *            运算的结果：  单词统计
 *                            result(单词 varchar, count int)
 *
 *            offsets：
 *                          offsets(group_id varchar, topic varchar, partition int , offset bigint)
 *
 *
 *           精准一次！借助事务！
 *
 */
object KafkaDirectStoreOffsetToMysql {

  val groupId:String ="pang"


  val streamingContext = new StreamingContext("local[2]", "wc", Seconds(5))

  def main(args: Array[String]): Unit = {



    // 消费者的配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ecs2:9092,ecs3:9092,ecs4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    // 查询之前已经在mysql中保存的offset
    val offsetsMap: Map[TopicPartition, Long] = readHitoryOffsetsFromMysql(groupId)

    //指定要消费主题
    val topics = Array("topic2")

    // 基于上次提交的offsets，构造一个DS，这个DS从上次提交的offsets位置向后消费的数据流
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetsMap)
    )



    ds.foreachRDD { rdd =>


      //消费到了数据
      if (!rdd.isEmpty()){
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //偏移量
        offsetRanges.foreach(println)

        //计算逻辑
        val result: Array[(String, Int)] = rdd.map(record => (record.value(), 1)).reduceByKey(_ + _).collect()


        // 开启事务，结果和offsets一起写入mysql
        writeResultAndOffsetsToMysql(result,offsetRanges)

      }
    }

    streamingContext.start()

    streamingContext.awaitTermination()


  }


  //从Mysql读取历史偏移量
  def readHitoryOffsetsFromMysql(groupId: String) : Map[TopicPartition, Long] = {

    //设置为可变 后续才能进行put操作
    //val offsetsMap: immutable.Map[TopicPartition, Long] = immutable.Map[TopicPartition, Long]()
    val offsetsMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    val sql:String=
      """
        |
        |SELECT
        |  `topic`,`partitionid`,`offset`
        |FROM `offset`
        |WHERE `groupid`=?
        |
        |
        |""".stripMargin


    try {

        conn=JDBCUtil.getConnection()

        ps=conn.prepareStatement(sql)

        ps.setString(1,groupId)

        rs= ps.executeQuery()

      while(rs.next()){

        val topic: String = rs.getString("topic")
        val partitionid: Int = rs.getInt("partitionid")
        val offset: Long = rs.getLong("offset")


        val topicPartition = new TopicPartition(topic, partitionid)

        offsetsMap.put(topicPartition,offset)

      }

    }catch {
      case e:Exception =>
        e.printStackTrace()
        throw new RuntimeException("查询偏移量出错！")

    }finally {

      if (rs != null){
        rs.close()
      }

      if (ps != null){
        ps.close()
      }

      if (conn != null){
        conn.close()
      }
    }

    //将可变map转为不可变map
    offsetsMap.toMap

  }



  /*
        在一个事务中，写入结果和偏移量

            当前是有状态的计算！
                第一批：  (a,2)

                第二批：  a,2
                          输出(a,4)

            语句？
                insert
                update

              #如果存在(通过主键来判断)就更新，如果不存在就插入   a-2 ---> a-->4 ,b-4
#VALUES(COUNT) 代表读取当前传入的count列的值  COUNT代表当前表中的count列的值

INSERT INTO `wordcount` VALUE('b',4) ON DUPLICATE KEY UPDATE `count`= COUNT + VALUES(COUNT);
   */
  def writeResultAndOffsetsToMysql(result: Array[(String, Int)], offsetRanges: Array[OffsetRange]): Unit = {

    val  sql1:String =
      """
        |
        |
        |INSERT INTO
        |    `wordcount` VALUES(?,?)
        |ON DUPLICATE KEY UPDATE `count`= COUNT + VALUES(COUNT)
        |
        |""".stripMargin


    val sql2:String =
      """
        |INSERT INTO
        |   `offset` VALUES(?,?,?,?)
        |   ON DUPLICATE KEY UPDATE `offset`= VALUES(OFFSET)
        |
        |
        |""".stripMargin


    var conn:Connection=null
    var ps1:PreparedStatement=null
    var ps2:PreparedStatement=null

    try {

      conn=JDBCUtil.getConnection()

      //取消事务的自动提交 ，只有取消了自动提交，才能将多次写操作组合为一个事务，手动提交
      conn.setAutoCommit(false)

      ps1=conn.prepareStatement(sql1)
      ps2=conn.prepareStatement(sql2)

      for ((word, count) <- result) {

        ps1.setString(1,word)
        ps1.setInt(2,count)


        ps1.addBatch()

      }

      //一批insert执行一次
      ps1.executeBatch()

      //模拟异常
      //1 / 0

      for (offsetRange <- offsetRanges) {

        ps2.setString(1,groupId)
        ps2.setString(2,offsetRange.topic)
        ps2.setInt(3,offsetRange.partition)
        ps2.setLong(4,offsetRange.untilOffset)

        ps2.addBatch()

      }

      ps2.executeBatch()

      //手动提交事务
      conn.commit()


    }catch {
      case e:Exception =>
        e.printStackTrace()

        //回滚事务
        conn.rollback()
        //重启app ，暂时以停止代替
        streamingContext.stop(true)

    }finally {

      if (ps1 != null){
        ps1.close()
      }

      if (ps2 != null){
        ps2.close()
      }

      if (conn != null){
        conn.close()
      }
    }


  }

}
