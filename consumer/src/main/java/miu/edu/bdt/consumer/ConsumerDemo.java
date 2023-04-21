package miu.edu.bdt.consumer;

import com.google.gson.Gson;
import miu.edu.bdt.consumer.dto.WeatherData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String grp_id = "demo_app";
        //Creating consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //Subscribing
        consumer.subscribe(Collections.singletonList(Constant.TOPIC_NAME));
        //polling
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Partition:" + record.partition() + ",Offset:" + record.offset());
                WeatherData data = gson.fromJson(record.value(), WeatherData.class);
                data.setZipcode(record.key());
                System.out.println(data);
            }
        }
    }
}