package cn.sysu.sql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before}

/**
  * @Author : song bei chang
  * @create 2021/3/18 20:38
  */
class JDBCRdd {

  var sc = new SparkContext(new SparkConf().setAppName("xiao_pang_Spark").setMaster("local[*]"))
  var OUT_PATH = "output"

  @Before
  def init: Unit ={
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(OUT_PATH)

    //如果目录存在,则删除
    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }

  @After
  def stop(): Unit ={
    sc.stop()
  }

  /**
    * 读写数据库mysql
    */
  @Test
  def hello1():Unit={
    val rdd = new JdbcRDD(
      sc,
      () => {
          Class.forName("com.mysql.jdbc.Driver")
          val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/java130?useSSL=false", "root", "root")
          connection
      },
      "select * from dept where deptno <= ? and deptno >=?",
      80,
      10,
      1,
      (rs: ResultSet) => {
          rs.getString("deptno") + " --- " + rs.getString("dname") + " --- " + rs.getString("loc")
      }
    )
    rdd.collect().foreach(println)
  }


  /**
    * 写数据库
    */
  @Test
  def hello2():Unit={
    val list = List((60,"xiao pang","nanjing"),(70,"xiao bao","fosan"))
    val rdd: RDD[(Int, String, String)] = sc.makeRDD(list,2)
    rdd.foreachPartition(it=>{
      Class.forName("com.mysql.jdbc.Driver")
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/java130?useSSL=false", "root", "root")
      val ps: PreparedStatement = connection.prepareStatement("insert into dept values (?,?,?)")

      it.foreach{
        case(deptno,dname,loc)=>{
          ps.setInt(1,deptno)
          ps.setString(2,dname)
          ps.setString(3,loc)
          ps.execute()
        }
      }
    })


  }
}
