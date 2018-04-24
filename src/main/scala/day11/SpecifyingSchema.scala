package day11

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ns1");
    sc.hadoopConfiguration.set("dfs.nameservices", "ns1");
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ns1", "nn1,nn2");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn1", "itcast03:9000");
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ns1.nn2", "itcast04:9000");
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    //sqlcontext
    val session = SparkSession.builder().appName("InferringSchema").master("local[*]").getOrCreate()
    //读取数据，切分
    val lineRDD = sc.textFile(args(0)).map(_.split(","))
    val schema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("faceValue",IntegerType,true)
      )
    )
    //映射Person对象,得到dataframe对象
    val rowRDD = lineRDD.map(x=> Row(x(0).toInt,x(1),x(2).toInt,x(3).toInt))

    val personDF = session.createDataFrame(rowRDD,schema)
    //注册临时表
    personDF.createTempView("t_person")
    //执行数据查询
    var df: DataFrame = session.sql("select * from t_person order by age desc")
    //数据保存
    df.write.json(args(1))

    sc.stop()

  }
}
