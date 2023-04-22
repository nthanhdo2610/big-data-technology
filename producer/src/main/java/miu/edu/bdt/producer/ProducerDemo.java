package miu.edu.bdt.producer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import miu.edu.bdt.producer.dto.Weather;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

public class ProducerDemo {
    private static final ProducerService service = ProducerService.getInstance();
    private static final Gson gson = new Gson();

    public static void main(String[] args) {

        // Zip code datasets
        List<String> datasets = service.getUsZip();
        List<List<String>> chunks = service.chunkData(datasets, Constant.CHUNK_SIZE);
        while (true) {

            for (List<String> zipcodes : chunks) {
                process(zipcodes);
            }
        }
    }

    static void process(List<String> zipcodes) {
        System.out.println("PROCESSING " + zipcodes.size() + " RECORDS!!!!!");
        // create the producer
        KafkaProducer<String, String> producer = service.getProducer();
        List<Weather> weathers = new ArrayList<>();
        for (String zip : zipcodes) {
            Weather weather = service.getWeatherData(zip);
            if (weather != null) {
                weathers.add(weather);
            }
        }
        service.publishData(
                producer,
                new ProducerRecord<>(Constant.TOPIC_NAME,
                        String.valueOf(System.currentTimeMillis()),
                        gson.toJson(weathers, new TypeToken<List<Weather>>() {
                        }.getType()))
        );

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
