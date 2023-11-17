package com.hpcc.streaminglab;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;

public class App {
    public static void main(String[] args) throws InterruptedException  {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.1.7.193:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "distinct_knam_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Name");
        Duration batchDuration = new Duration(1000);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
        jssc.sparkContext().setLogLevel("WARN");
        jssc.checkpoint("/tmp/2153599/");
        Collection<String> topics = Arrays.asList("topic_2153599");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
          KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
          );
        //stream.print();
        //jssc.start();
        //jssc.awaitTermination();
        stream.map(record->(record.value().toString())).print(); 
        jssc.start(); 
        jssc.awaitTermination();

    }
}
