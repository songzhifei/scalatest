package day04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;

public class StreamingOperator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("StreamingOperator").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.hadoopConfiguration().set("fs.defaultFS", "hdfs://ns1");
        sc.hadoopConfiguration().set("dfs.nameservices", "ns1");
        sc.hadoopConfiguration().set("dfs.ha.namenodes.ns1", "nn1,nn2");
        sc.hadoopConfiguration().set("dfs.namenode.rpc-address.ns1.nn1", "itcast02:9000");
        sc.hadoopConfiguration().set("dfs.namenode.rpc-address.ns1.nn2", "itcast03:9000");
        sc.hadoopConfiguration().set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.112.133", 8888);

        ssc.checkpoint("hdfs://ns1/ck-20180424");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.print();

        ssc.start();

        ssc.awaitTermination();

        ssc.stop(false);

    }
}
