package com.hpcc.streaminglab;

import org.apache.spark.streaming.Duration;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;
import scala.Tuple2;

public class App {
	public static void main(String[] args) throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "10.1.7.194:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "knam_distinct_group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("IoTStream");
		Duration batchDuration = new Duration(300000); // 5 minutes
		JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
		jssc.sparkContext().setLogLevel("WARN");
		jssc.checkpoint("/user/S2153599/test2/");
		Collection<String> topics = Arrays.asList("test_2153599");
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
						.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


 		// Maps each Kafka record to a Tuple2 of humidity and temperature.
		JavaDStream<Tuple2<Double, Double>> humidityAndTemperatureStream = stream.map(record -> {
			// Extracting JSON data from Kafka record
			byte[] valueBytes = record.value().getBytes(StandardCharsets.UTF_8);
			String jsonString = new String(valueBytes, StandardCharsets.UTF_8);

			ObjectMapper objectMapper = new ObjectMapper();
			try {
				// Parsing JSON values for humidity and temperature
				JsonNode jsonNode = objectMapper.readTree(jsonString);
				double humidity = jsonNode.get("humidity").asDouble();
				double temperature = jsonNode.get("temperature").asDouble();
				return new Tuple2<>(humidity, temperature);
			} catch (Exception e) {
				// Handling JSON parsing exception, filtering out the record
				return null;
			}
		}).filter(record -> record != null);


		/**
		* Calculates the average humidity and temperature over a specified time window.
		* Maps each humidity and temperature value to a Tuple2 with count initialized to 1.
		* Reduces by key to accumulate values and counts.
		* Maps the result to a Tuple2 with the calculated average and total count.
 		**/
		JavaDStream<Tuple2<Double, Double>> averagedHumidityStream = humidityAndTemperatureStream
                        .mapToPair(tuple -> new Tuple2<>(tuple._1, 1.0))
                        .reduce((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .map(sum -> new Tuple2<>(sum._1 / sum._2, sum._2));

		JavaDStream<Tuple2<Double, Double>> averagedTemperatureStream = humidityAndTemperatureStream
                        .mapToPair(tuple -> new Tuple2<>(tuple._2, 1.0))
                        .reduce((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .map(sum -> new Tuple2<>(sum._1 / sum._2, sum._2));

		// Print out results
		averagedHumidityStream.print();
		averagedTemperatureStream.print();

		jssc.start();
		jssc.awaitTermination();
	}
}
