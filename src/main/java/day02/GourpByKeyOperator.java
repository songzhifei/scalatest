package day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

public class GourpByKeyOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("GourpByKeyOperator").setMaster("local");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = sc.textFile("E:\\words.txt");

        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID=1L;

            public Tuple2<String, Integer> call(String str) throws Exception {
                String[] splited = str.split("\t");
                String className = splited[0];
                Integer score = Integer.valueOf(splited[1]);
                return  new Tuple2<String, Integer>(className,score);

            }
        });
        pairRDD.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

            private static final long serialVersionUID=1L;
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {

                String className = tuple._1;
                Iterator<Integer> iterator = tuple._2.iterator();
                Integer[] top3 = new Integer[3];

                while (iterator.hasNext()){

                    Integer score = iterator.next();

                    for(int i=0;i<top3.length;i++){

                        if(top3[i] == null){
                            top3[i] =score;
                            break;
                        }else if(score>top3[i]){
                            for(int j=2;j>i;j--){

                                top3[j] = top3[j-1];

                            }
                            top3[i] = score;

                            break;
                        }
                    }
                }
                System.out.println("------------------");
                System.out.println("className:"+className+",score:");
                for(int i=0;i<top3.length;i++){

                    System.out.println(top3[i]);
                }
                System.out.println("------------------");
            }
        });
        sc.stop();
    }
}
