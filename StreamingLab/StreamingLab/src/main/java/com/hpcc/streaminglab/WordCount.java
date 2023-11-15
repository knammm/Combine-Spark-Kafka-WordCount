package com.hpcc.streaminglab;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.SparkConf;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
        public static void main(String[] args) throws TimeoutException, StreamingQueryException {
                SparkConf conf = new SparkConf().setMaster("yam").setAppName("WordCount");
                SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
                spark.sparkContext().setLogLevel("WARN");
                Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "10.1.7.194:9092").option("subscribe", "topic_2153599").load();
                df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
                df = df.groupBy("value").count();
                StreamingQuery query = df.writeStream().option("checkpointLocation", "/user/S2153599/test1/").outputMode("update").format("console").start();
                query.awaitTermination();
        }
}
