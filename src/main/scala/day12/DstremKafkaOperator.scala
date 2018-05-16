package day12

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DstremKafkaOperator {
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

    ssc.checkpoint("hdfs://ns1/ck-20180516")

    val topics = Array("customerCountries") //我们需要消费的kafka数据的topic
    val kafkaParam = Map[String,Object](
      "metadata.broker.list" -> "itcast02:9092,itcast03:9092", // kafka的broker list地址
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "bootstrap.servers"-> "itcast02:9092,itcast03:9092"
    )

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics,kafkaParam))

    val tuples = stream.map(_.value()).flatMap(_.split(" ")).map((_,1))

    var res: DStream[(String, Int)] = tuples.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), false)

    res.print()

    ssc.start()

    ssc.awaitTermination()
  }
  val func = (it:Iterator[(String,Seq[Int],Option[Int])])=>{
    it.map(t=>{
      (t._1,t._2.sum + t._3.getOrElse(0))
    })
  }
}
