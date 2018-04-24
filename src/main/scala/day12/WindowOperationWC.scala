package day12

import day12.StreamingWC.func
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object WindowOperationWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ns1");
    sc.hadoopConfiguration.set("dfs.nameservices", "ns1");
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ns1", "nn1,nn2");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn1", "itcast02:9000");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn2", "itcast03:9000");
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("hdfs://ns1/ck-20180424")
    //监听数据
    val dstream = ssc.socketTextStream("192.168.112.133",8888)

    val tuples = dstream.flatMap(_.split(" ")).map((_,1))
    //第一个seconds为窗口长度，第二个为滑动长度，必须为RDD（5）的倍数
    var res: DStream[(String, Int)] = tuples.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(10),Seconds(10))

    res.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
