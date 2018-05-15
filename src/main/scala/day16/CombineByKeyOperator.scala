package day16

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CombineByKeyOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CombineByKeyOperator").setMaster("local")

    val sc  = new SparkContext(conf)

    val unit = sc.parallelize(Array(("A",1),("B",2),("C",4),("D",3),("B",1),("E",5)),2)

    val partionsRDD = unit.mapPartitionsWithIndex((index, iterator) => {
      val list = new ListBuffer[(String, Int)]
      while (iterator.hasNext) {
        val res = iterator.next()
        list += res
      }
      list.iterator
    }, false)

    //partionsRDD.combineByKey((x)=>x+"~",(x:String,y:Int)=>x+"@"+y,(x:String,y:String)=>x+"$"+y)

    partionsRDD.combineByKey((x)=>ListBuffer(x),(x:ListBuffer[Int],y:Int)=>x+=y,(x:ListBuffer[Int],y:ListBuffer[Int])=>x ++=y).foreach{println}

  }
}
