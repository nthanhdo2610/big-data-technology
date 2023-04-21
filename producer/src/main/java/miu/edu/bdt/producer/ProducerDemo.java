package miu.edu.bdt.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static final ProducerService service = new ProducerService();

    public static void main(String[] args) {

        // Zip code datasets
        Set<String> set = service.getUsZip();

        //create a list to hold the Future object associated with Callable
        List<Future<ProducerRecord<String, String>>> list = new ArrayList<>();


        while (true) {

            // create the producer
            KafkaProducer<String, String> producer = service.createProducer();

            for (String zip : set) {
                String data = service.getWeatherData(zip);

                //submit Callable tasks to be executed by thread pool
                Future<ProducerRecord<String, String>> future = executor.submit(() -> service.publishData(producer, new ProducerRecord<>(Constant.TOPIC_NAME, zip, data)));

                //add Future to the list, we can get return value using Future
                list.add(future);
            }

            for (Future<ProducerRecord<String, String>> fut : list) {
                try {
                    //print the return value of Future, notice the output delay in console
                    // because Future.get() waits for task to get completed
                    fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            // flush data - synchronous
            producer.flush();

            // flush and close producer
            producer.close();

            try {
                Thread.sleep(15000);
            } catch (InterruptedException ignored) {

            }
        }
    }
}
