package day11

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object InferringSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ns1");
    sc.hadoopConfiguration.set("dfs.nameservices", "ns1");
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ns1", "nn1,nn2");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn1", "itcast03:9000");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn2", "itcast04:9000");
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    val session = SparkSession.builder().appName("InferringSchema").master("local[*]").getOrCreate()

    val lineRDD = sc.textFile(args(0)).map(_.split(","))

    val personRDD = lineRDD.map(x=>Person(x(0).toInt,x(1),x(2).toInt,x(3).toInt))

    import session.implicits._
    val personDF = personRDD.toDF

    personDF.createTempView("t_person")

    var df: DataFrame = session.sql("select * from t_person order by age desc limit 2")

    df.write.json(args(1))

    sc.stop()
  }
}
case class Person(id:Int,name:String,age:Int,faceValue:Int)