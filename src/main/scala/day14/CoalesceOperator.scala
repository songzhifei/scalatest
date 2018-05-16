package day14

import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class CoalesceOperator {

}
object CoalesceOperator{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local")

    val sc = new SparkContext(conf)

    val unit = sc.makeRDD(Array(
      "Anglebaby1",
      "Anglebaby2",
      "Anglebaby3",
      "Anglebaby4",
      "Anglebaby5",
      "Anglebaby6",
      "Anglebaby7",
      "Anglebaby8",
      "Anglebaby9",
      "Anglebaby10",
      "Anglebaby11",
      "Anglebaby12"
    ),6)
//    unit.mapPartitionsWithIndex((index,iterator)=>{
//      val list = new ListBuffer[String]
//      while (iterator.hasNext)
//      {
//        val num = iterator.next()
//        println("index:"+index+",value:"+num)
//        list.+=(num)
//      }
//      list.iterator
//    },false).count()
    unit.coalesce(3,false).mapPartitionsWithIndex((index,iterator)=>{
            val list = new ListBuffer[String]
            while (iterator.hasNext)
            {
              val num = iterator.next()
              println("index:"+index+",value:"+num)
              list.+=(num)
            }
            list.iterator
          },false).count()
    sc.stop()
  }

}
