package miu.edu.bdt.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerDemo {

    private static final Gson gson = new Gson();
    private static final ConsumerService service = ConsumerService.getInstance();

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String grp_id = "demo_app";

        //Creating consumer properties
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //creating consumer
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                new SparkConf().setAppName("SparkStreaming").setMaster("local[*]"), new Duration(250));

        //Subscribing
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singleton(Constant.TOPIC_NAME), properties)
                );

        // Insert data into Hive from Kafka
        stream.foreachRDD(rdd -> rdd.foreach(record -> {
            if (record != null) {
                logger.info("Partition:" + record.partition() + ",Offset:" + record.offset());
                try {
                    List<Weather> weathers = gson.fromJson(record.value(), new TypeToken<List<Weather>>() {
                    }.getType());
                    service.batchInsert(weathers);
                } catch (JsonSyntaxException e){
                    e.printStackTrace();
                }
            }
        }));

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    
}