/**
  * @Author : song bei chang
  * @create 2021/6/7 11:42
  */


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test



case class Person(name:String,age:Int)
class SQL {


  var session:SparkSession = null

  @Test
  def testRDDToDf() : Unit ={

    val sparkSession: SparkSession = SparkSession.builder.getOrCreate()

    val list = List(Person("jack", 20), Person("marry", 30))

    val rdd: RDD[Person] = session.sparkContext.makeRDD(list)

    import  sparkSession.implicits._

    //implicit def rddToDatasetHolder[T : Encoder](rdd: RDD[T]): DatasetHolder[T]
    //DatasetHolder: A container for a [[Dataset]], used for implicit conversions in Scala.
    // rdd: RDD[Person] ----->rddToDatasetHolder(rdd) ----> DatasetHolder[Person]  ----------->DatasetHolder[Person].toDF()
    val df: DataFrame = rdd.toDF()

    val ds: Dataset[Person] = rdd.toDS()

  }

}
