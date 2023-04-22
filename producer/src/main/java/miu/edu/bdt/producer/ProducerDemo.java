package miu.edu.bdt.producer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import miu.edu.bdt.producer.dto.Weather;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ProducerDemo {
    private static final ProducerService service = ProducerService.getInstance();
    private static final Gson gson = new Gson();
    private static final ExecutorService executor = Executors.newFixedThreadPool(5);

    public static void main(String[] args) throws InterruptedException {
        // Zip code datasets
        List<String> datasets = service.getUsZip();
        List<List<String>> chunks = service.chunkBySize(datasets, Constant.SIZE_CHUNK);
        while (true) {
            List<Future<String>> futures = new ArrayList<>();
            for (List<String> zipcodes : chunks) {
                Future<String> future = executor.submit(()->process(zipcodes));
                futures.add(future);
            }
            for(Future<String> fut : futures){
                try {
                    System.out.println(fut.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            Thread.sleep(60000);
        }
    }

    static String process(List<String> zipcodes) {
        System.out.println("PROCESSING " + zipcodes.size() + " RECORDS!!!!!");
        List<Weather> weathers = new ArrayList<>();
        for (String zip : zipcodes) {
            Weather weather = service.getWeatherData(zip);
            if (weather != null) {
                weathers.add(weather);
            }
        }

        // create the producer
        KafkaProducer<String, String> producer = service.getProducer();
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
        System.out.println("PROCESSED " + zipcodes.size() + " RECORDS!!!!!");
        return "";
    }
}
