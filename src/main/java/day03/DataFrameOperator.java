package day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class DataFrameOperator {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("DataFrameOperator").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> line = sc.textFile("people.txt");
        //构建rowRDD
        JavaRDD<Row> rowRDD = line.map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;

            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });
        //构建schme
        ArrayList<StructField> structFields = new ArrayList<StructField>();

        structFields.add(DataTypes.createStructField("id",IntegerType,true));

        structFields.add(DataTypes.createStructField("name",StringType,true));

        structFields.add(DataTypes.createStructField("age",IntegerType,true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRDD, structType);

        dataFrame.registerTempTable("people");

        Dataset<Row> row = sqlContext.sql("select * from people where age>10");

        row.show();

        sc.stop();
    }
}
