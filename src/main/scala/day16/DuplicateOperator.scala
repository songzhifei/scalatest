package day16

import org.apache.spark.{SparkConf, SparkContext}

object DuplicateOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DuplicateOperator").setMaster("local")

    val sc = new SparkContext(conf)

    val line1 = sc.textFile("Duplicate1.txt")
    val line2 = sc.textFile("Duplicate2.txt")


    val res1 = line1.filter(_.trim().length()>0).map((_,""))

    val res2 = line2.filter(_.trim().length()>0).map((_,""))

    res1.union(res2).groupByKey().sortByKey().keys.foreach(println)

  }
}
