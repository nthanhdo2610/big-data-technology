package miu.edu.bdt.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KProducer {

	private static final Logger logger = LoggerFactory.getLogger(KProducer.class);
	private static final String DEFAULT_INPUT = "weatherAUS.csv";
	private static final ProducerService service = new ProducerService();

	public static void main(String[] args) {
		String input = args.length > 0 ? args[0] : null;
		if (input == null) {
			input = DEFAULT_INPUT;
		}

		publishMessages(input);
	}

	private static void publishMessages(String input) {
		try {
			List<String> lines = Files.lines(Paths.get(input)).skip(1).collect(Collectors.toList());
			publish(lines);
		} catch (IOException e) {
			logger.error("Cannot read input file. " + e);
		}
	}

	private static void publish(List<String> data) throws IOException {
		// create the producer
		Producer<String, String> producer = service.createProducer();
        
        data.stream().map(d -> new ProducerRecord<String, String>(Constant.TOPIC_NAME, d)).forEach(record -> {
			try {
				producer.send(record).get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Cannot send record. " + e);
			}
		});
        
		// flush data - synchronous
		producer.flush();
		// flush and close producer
		producer.close();
	}

}
