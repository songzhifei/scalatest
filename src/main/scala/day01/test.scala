package day01

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Sparktest").setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    val words = sc.parallelize(List("a b c","d e f","m n j"))

    val length = words.map(s=>s.length)
    //    println(length.collect().toBuffer)
    val i: Int = length.reduce((a, b)=>a+b)
    println(i)
    //val res1 = words.flatMap(_.split(" "))

    //println(res1.collect().toBuffer)
  }
}
