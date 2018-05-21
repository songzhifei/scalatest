package day04;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;

public class JDBCDataSouce {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("JDBCDataSouce").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrameReader reader = sqlContext.read().format("jdbc");

        reader.option("url","jdbc:mysql://localhost:3306/sakila?useSSL=false");
        reader.option("dbtable","actor");
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","root");

        Dataset<Row> load = reader.load();

        load.registerTempTable("actor");

        String sql = "select * from actor";

        Dataset<Row> sql1 = sqlContext.sql(sql);

        sql1.show();


         sql1.javaRDD().foreach(new VoidFunction<Row>() {

              public void call(Row row) throws Exception {
                  String sql = "insert into actor_bak values("+Integer.valueOf(String.valueOf(row.get(0)))+",'"
                          +String.valueOf(row.get(1))+"','"
                          +String.valueOf(row.get(2))+"','"
                          +String.valueOf(row.get(3))+ "')";
                  Class.forName("com.mysql.jdbc.Driver");

                  Connection conn = null;

                  Statement status = null;
                  try {
                      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sakila?useSSL=false&user=root&password=root");

                      status = conn.createStatement();

                      status.executeUpdate(sql);

                  }catch (Exception ex){
                      ex.printStackTrace();
                  }finally {
                      if(status!=null){
                          status.close();
                      }
                      if(conn!=null){
                          conn.close();
                      }
                  }
             }
          });

    }
}
