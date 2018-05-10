package day14

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
class MyAccumulator extends AccumulatorV2[Int,Int]{
  private var res = 0

  override def isZero: Boolean = {true}

  override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case o : MyAccumulator => res += o.res
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def copy(): MyAccumulator = {
    val newMyAcc = new MyAccumulator
    newMyAcc.res = this.res
    newMyAcc
  }

  override def value: Int = res

  override def add(v: Int): Unit = {

    res += v
  }

  override def reset(): Unit = {
    res = 0
  }

}
object BroadCastOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local")

    val sc = new SparkContext(conf)

    val unit = sc.makeRDD(1 to 10,3)

    val i = 10
    //广播变量
    val bc = sc.broadcast(i)
    //累加器
    //val value = sc.accumulator(0)
    val myAccumulator = new MyAccumulator
    sc.register(myAccumulator,"myAccumulator")
    unit.foreach(x=>{
      myAccumulator.add(1)
    })
    println(myAccumulator.value)
    /*
    *
    *
      unit.filter(x=>{
      val b = bc.value
      x != b
    }).foreach{println}
    * */


    sc.stop()
  }
}

