package day07

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "G:\\tools\\hadoop\\hadoop-2.7.5\\hadoop-2.7.5")

    val conf = new SparkConf().setAppName("SparkRDDTest").setMaster("local")

    val sc= new SparkContext(conf)

    val rdd2 = sc.parallelize(Array("a v c","e o g"))

    //val res = rdd2.flatMap(_.split(" "))
    //println(res.collect().toBuffer)
    val rdd3 = sc.parallelize(List(List("a b c","v n e"),List("a b c","v n e"),List("a b c","v n e")))

    val res1 = rdd3.flatMap(_.flatMap(_.split(" ")))
    println(res1.collect.toBuffer)
    //求并集 union
//    val rdd4 = sc.parallelize(List(1,6,4,77,33))
//    val rdd5 = sc.parallelize(List(1,4,5,5,6,7))
//    println((rdd4 union rdd5).collect().toBuffer)
    //求交集 rdd2.intersection()
    //去重 rdd2.distinct()
//join leftOuterJoin rightOuterJoin




    sc.stop()
  }
}
