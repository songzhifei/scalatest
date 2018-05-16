package day06
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    var lines = sc.textFile(args(0))

    var words: RDD[String] = lines.flatMap(_.split(" "))

    var paird: RDD[(String, Int)] = words.map((_, 1))

    var reduced: RDD[(String, Int)] = paird.reduceByKey(_ + _)

    var res: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    res.collect().foreach{println}
    //println(res.collect().toBuffer)

    sc.stop()
  }
}
