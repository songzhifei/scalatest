package day12

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object mapPartitionsWithIndexRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionsWithIndexRDD").setMaster("local")

    val sc  = new SparkContext(conf)

    val unitRDD = sc.makeRDD(1 to 10,3)

    val mapPartitionsWithIndexRDD = unitRDD.mapPartitionsWithIndex((index, iterator) => {
      val list = new ListBuffer[Int]
      while (iterator.hasNext) {
        val num = iterator.next()
        println("partitionId:" + index + "value:" + num)
        list += num
      }
      list.iterator
    }, false)

    val res = mapPartitionsWithIndexRDD.collect()

    //res.foreach { println }

    sc.stop()

  }
}
