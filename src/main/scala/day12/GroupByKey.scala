package day12

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")

    val sc  = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(
      Tuple2("A", 1),
      Tuple2("B", 2),
      Tuple2("B", 3),
      Tuple2("C", 4),
      Tuple2("D", 5)
    ))
    val res = rdd.groupByKey()

    res.collect().foreach {println}

    sc.stop()
  }
}
