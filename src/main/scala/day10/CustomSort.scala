package day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mySort{
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue!=y.faceValue){
        x.faceValue-y.faceValue
      }else{
        y.age-x.age
      }
    }
  }
}
object CustomSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("customSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.parallelize(Array(("tingting",90,27),("ningning",90,26),("lili",80,25)))

    //val res = lines.sortBy(_._2,false)
    //自定义排序：第一种，重写ordering中的compare方法
    //    import mySort.girlOrdering
    //    val res: RDD[(String, Int, Int)] = lines.sortBy(x => Girl(x._2, x._3), false)
    //自定义排序：第二种，排序类继承Ordered类，重写compare方法
    var res: RDD[(String, Int, Int)] = lines.sortBy(x => Girl(x._2, x._3), false)
    println(res.collect.toBuffer)

  }
}
//case class Girl(faceValue:Int,age:Int){}
case class Girl(faceValue:Int,age:Int) extends Ordered[Girl]{
  override def compare(that: Girl): Int = {
    if(this.faceValue!=that.faceValue){
      this.faceValue-that.faceValue
    }else{
      that.age-this.age
    }
  }
}

