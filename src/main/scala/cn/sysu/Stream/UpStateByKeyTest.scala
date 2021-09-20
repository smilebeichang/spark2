package cn.sysu.Stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Created by Smexy on 2021/7/3
 *
 *    State（状态）：  每个批次计算结束后要保存的变量(结果)
 *
 *    有状态的计算：  当前批次数据计算的结果，作为状态，进行保存！保存后用于下一个批次的计算！
 *                  下一个批次在计算时，会使用到上一个批次计算的结果！
 *
 *                      核心： 需要将每个批次计算的状态进行保存
 *
 *                        累积统计，常出现的场景： 统计从xx时间点开始，到现在的所有的数据
 *
 *                   算子： 名称都会以  xxxxStatexxx 为形式命名
 *
 *    无状态的计算：  昨天写的所有的案例，都是无状态的计算。
 *                    每个批次的计算是相互独立的！
 *
 *
 *
 * def updateStateByKey[S: ClassTag](
 *        updateFunc: (Seq[V], Option[S]) => Option[S] :
 *              Seq[V]: 当前批次每个K对应的values
 *              Option[S]: 每个K对于的state
 *
 *              updateFunc: 将当前批次每个k的values和之前的state进行运算，得到新的state
 *
 *              返回值是Option[S]： 状态。
 *                    确定返回状态，使用Some()
 *                    不想返回状态，使用None()
 * ): DStream[(K, S)]
 *
 *
 * 报错： requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint()
 *
 *      在spark中，所有带 xxxstate的算子，都需要设置 ck目录！
  *      could only be written to 0 of the 1 minReplication nodes. There are 3 datanode(s) running and 3 node(s) are excluded in this operation.
 */
object UpStateByKeyTest {

  def main(args: Array[String]): Unit = {

    val streamingContext = new StreamingContext("local[2]","wc", Seconds(5))

    // 设置保存状态的ck目录  一般是一个HDFS的路径，现在测试，可以使用本地目录 "ck"
    //返回的IP地址无法返回公网IP，所以通过设置让其返回主机名，通过主机名与公网地址的映射便可以访问到DataNode节点，问题将解决。
    // .config("dfs.client.use.datanode.hostname", "true")
    // .config("dfs.replication", "2")
    streamingContext.checkpoint("hdfs://ecs2:9820/ck")

    val ds: ReceiverInputDStream[String] = streamingContext.socketTextStream("ecs4", 9998)

    val ds2: DStream[(String, Int)] = ds.map((_, 1))

    //  DStream[(K, v)]  隐式转换为  PairDStreamFunctions
    val result: DStream[(String, Int)] = ds2.updateStateByKey(
      (newValues: Seq[Int], state: Option[Int]) => Some(newValues.sum + state.getOrElse(0))
    )

    result.print(1000)

    streamingContext.start()

    streamingContext.awaitTermination()


  }

}
