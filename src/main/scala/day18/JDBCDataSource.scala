package day18

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")

    //val sc = new SparkContext(conf)

    val session = SparkSession.builder().config(conf).getOrCreate()

    //定义mysql信息
    val jdbcDF = session.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/sakila",
        "dbtable"->"actor",
        "driver"->"com.mysql.jdbc.Driver",
        "user"-> "root",
        "password"->"root")).load()

    jdbcDF.createTempView("actor")

    val dataFrame = session.sql("SELECT `actor_id`, `first_name`,`last_name`,`last_update` FROM `actor`")

    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/sakila","actor_bak",prop)

    session.stop()
  }

}
