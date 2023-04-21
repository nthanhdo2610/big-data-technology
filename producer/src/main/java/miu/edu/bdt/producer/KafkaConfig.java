package miu.edu.bdt.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConfig {
	
	public static final String KAFKA_BROKERS = "quickstart.cloudera:9092";
	public static final Integer MESSAGE_COUNT = 1000;
	public static final String TOPIC_NAME = "weather_topic";
	public static final Integer MESSAGE_SIZE = 20971520;
	
	public static Producer<String, String> createProducer() {
		// create Producer properties
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_BROKERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, KafkaConfig.MESSAGE_SIZE);
		
		return new KafkaProducer<>(props);
	}

}
