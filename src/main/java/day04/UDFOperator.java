package day04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UDFOperator {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("UDFOperator").setMaster("local");

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

        sqlContext.udf().register("strLen", new UDF1<String, Integer>() {

            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        Dataset<Row> sql = sqlContext.sql("select first_name,strLen(first_name) from actor where actor_id<20");

        sql.show();


    }
}
