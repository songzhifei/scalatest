package day18

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UDFOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")

    //val sc = new SparkContext(conf)

    val session = SparkSession.builder().config(conf).getOrCreate()

    //定义mysql信息
    val jdbcDF = session.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/sakila?useSSL=false",
        "dbtable"->"actor",
        "driver"->"com.mysql.jdbc.Driver",
        "user"-> "root",
        "password"->"root")).load()

    jdbcDF.createTempView("actor")
    //udf user defined function自定义聚合函数
    session.udf.register("strLen",(str:String)=>str.length())
    //session.sql("select first_name,strLen(first_name) from actor where actor_id<20").show()
    //udaf  user defined Aggregate function自定义聚合函数
    session.udf.register("strCount",new StringCount)
    session.sql("select first_name,strCount(first_name) from actor group by first_name").show()


  }
}
