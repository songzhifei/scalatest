package day17

import java.util
import java.util.ArrayList

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataFrameOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataFrameOperator").setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    val session = SparkSession.builder().appName("DataFrameOperator").master("local").getOrCreate()

    val line = sc.textFile("people.txt")
    //构建lineRDD
    val lineRDD = line.map(_.split(","))
    //构建rowRDD
    val rowRDD = lineRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    //构建schme
    val schma = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )
    //构建df
    val df = session.createDataFrame(rowRDD,schma)
    //创建临时表
    df.createTempView("people")
    //执行sql
    val row: Dataset[Row] = session.sql("select * from people where age>10")
    //
    row.show()

    sc.stop()
  }
}
