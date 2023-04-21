package miu.edu.bdt.producer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import miu.edu.bdt.producer.dto.Weather;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static final ProducerService service = new ProducerService();
    private static final Gson gson = new Gson();

    public static void main(String[] args) {

        // Zip code datasets
        List<String> datasets = service.getUsZip();
        List<List<String>> chunks = service.chunkData(datasets, Constant.CHUNK_SIZE);
        while (true) {

            //create a list to hold the Future object associated with Callable
            List<Future<Integer>> futures = new ArrayList<>();

            for (List<String> zipcodes : chunks) {
                //submit Callable tasks to be executed by thread pool
                Future<Integer> future = executor.submit(() -> process(zipcodes));

                //add Future to the list, we can get return value using Future
                futures.add(future);
            }

            for (Future<Integer> data : futures) {
                try {
                    //print the return value of Future, notice the output delay in console
                    // because Future.get() waits for task to get completed
                    data.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static int process(List<String> zipcodes) {

        // create the producer
        KafkaProducer<String, String> producer = service.createProducer();
        List<Weather> weathers = new ArrayList<>();

        //create a list to hold the Future object associated with Callable
        List<Future<Weather>> dataFutures = new ArrayList<>();

        for (String zip : zipcodes) {

            //submit Callable tasks to be executed by thread pool
            Future<Weather> future = executor.submit(() -> service.getWeatherData(zip));

            //add Future to the list, we can get return value using Future
            dataFutures.add(future);
        }

        for (Future<Weather> data : dataFutures) {
            try {
                //print the return value of Future, notice the output delay in console
                // because Future.get() waits for task to get completed
                Weather weather = data.get();
                if (weather != null) {
                    weathers.add(weather);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
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

        return 1;
    }
}
