package day16

import org.apache.spark.{SparkConf, SparkContext}

object PracticeOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PracticeOperator").setMaster("local")

    val sc  = new SparkContext(conf)

    val lineRDD = sc.textFile("wagzhe.txt")

    val mapRDD = lineRDD.map(_.split(","))

    val map2RDD = mapRDD.map(a=>(a(0)+","+a(1),a(2).toInt))

    val resRDD = map2RDD.reduceByKey(_+_).map(a=>(a._2,a._1)).sortByKey(false).map(a=>(a._2,a._1))

    resRDD.foreach{println}
    //println(resRDD.collect().toBuffer)
  }
}
