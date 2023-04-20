package miu.edu.bdt.producer;

import au.com.bytecode.opencsv.CSVReader;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final OkHttpClient client = new OkHttpClient();

    public static void main(String[] args) {
        String bootstrapServers = "quickstart.cloudera:9092";
        String topic = "weather_topic";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Zip code datasets
        Set<String> set = getUsZip();

        while (true) {
            // create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            for (String zip : set) {
                String data = getWeatherData(zip);
                if (!data.isEmpty()) {
                    executorService.execute(() -> publishData(producer, new ProducerRecord<>(topic, zip, data)));
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

    static Set<String> getUsZip() {
        CSVReader reader = null;
        Set<String> zips = new HashSet<>();
        try {
            InputStream is = ProducerDemo.class.getResourceAsStream("/us_zip_codes_test.csv");
            if (is == null) {
                throw new IOException();
            }
            reader = new CSVReader(new InputStreamReader(is));
            String[] line;
            while ((line = reader.readNext()) != null) {
                String code = "";
                int zip = Integer.parseInt(line[0]);
                if (zip < 1000) {
                    code = "00" + zip;
                } else if (zip < 10000) {
                    code = "0" + zip;
                } else {
                    code = line[0];
                }
                zips.add(code);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return zips;
    }

    static String getWeatherData(String zipcode) {
        try {
            Request request = new Request.Builder()
                    .url("https://weatherapi-com.p.rapidapi.com/current.json?q=" + zipcode)
                    .get()
                    .addHeader("X-RapidAPI-Key", "35ae57a75dmshfa432d963090737p1251cfjsn78a4b5dbd63a")
                    .addHeader("X-RapidAPI-Host", "weatherapi-com.p.rapidapi.com")
                    .build();
            Response response = client.newCall(request).execute();
            if (response.code() == 200) {
                return Objects.requireNonNull(response.body()).string();
            } else {
                throw new Exception(response.message());
            }
        } catch (Exception e) {
            log.error("GET Weather data by zip " + zipcode + " error " + e.getMessage());
        }
        return "";
    }

    static void publishData(KafkaProducer<String, String> producer, ProducerRecord<String, String> producerRecord) {
        // send data - asynchronous
        producer.send(producerRecord, (recordMetadata, e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
                // the record was successfully sent
                log.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Key:" + producerRecord.key() + "\n" +
                        "Value:" + producerRecord.value() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e);
            }
        });
    }
}
