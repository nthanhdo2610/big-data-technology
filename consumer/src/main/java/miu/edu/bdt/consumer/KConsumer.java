package miu.edu.bdt.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class KConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KConsumer.class);
    public static final String TOPIC_NAME = "weather_topic";

    public static void main(String[] args) {
        startConsumer();
    }

    private static void startConsumer() {
        try {
            HiveRepository repo = HiveRepository.getInstance();

            JavaStreamingContext streamingContext = new JavaStreamingContext(
                    new SparkConf().setAppName("SparkStreaming").setMaster("local[*]"), new Duration(250));

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "group");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            Collection<String> topics = Collections.singletonList(TOPIC_NAME);

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topics, kafkaParams)
                    );

            // Insert data into Hive from Kafka
            stream.foreachRDD(rdd -> rdd.foreach(x -> {
                WeatherRecord record = RecordParser.parse(x.value());
                if (record != null) {
                    System.out.println(x.value());
                    System.out.println(record);
                    repo.insertRecord(record);
                }
            }));

            streamingContext.start();
            streamingContext.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
