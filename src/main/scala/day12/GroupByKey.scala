package day12

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")

    val sc  = new SparkContext(conf)
    /**
      * val rdd = sc.makeRDD(Array(
      * Tuple2("A", 1),
      * Tuple2("B", 2),
      * Tuple2("B", 3),
      * Tuple2("C", 4),
      * Tuple2("D", 5)
      * ))
      */
    val lineRDD = sc.textFile("E:\\words.txt")

    //想要进行groupbykey，reducebykey，必须转换成keyvalue的格式
    val rdd = lineRDD.map(a=>{
      val splited = a.split("\t")
      (splited(0),splited(1).toInt)
    })

    val res = rdd.groupByKey().map(classScore=>{
      val className = classScore._1
      val scores = classScore._2
      val top3 = Array[Int](0,0,0)
      for(score <- scores){
        breakable{
          for(i <- 0 until 3){
            if(top3(i)==0){
              top3(i) = score
              break()
            }else if(score>top3(i)){
              var j=2
              while (j>i){
                top3(j) = top3(j-1)
                j = j-1
              }
              top3(i) = score
              break()
            }
          }
        }
      }
      (className,top3)
    })

    res.collect().foreach(x=>{
      println(x._1)
      val res = x._2
      for (i <- res) {
        println(i)
      }
      println("==========================")
    })

    sc.stop()
  }
}
