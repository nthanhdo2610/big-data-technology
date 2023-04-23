package miu.edu.bdt.producer;

import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.Gson;
import miu.edu.bdt.producer.dto.Weather;
import miu.edu.bdt.producer.dto.WeatherData;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class ProducerService {

    private static ProducerService INSTANCE = null;
    private static final OkHttpClient client = new OkHttpClient();
    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);
    private static final Gson gson = new Gson();
    private static final Set<String> invalidZipcodes = new HashSet<>();
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constant.HIVE_TIMESTAMP_FORMAT);

    public static ProducerService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProducerService();
        }
        return INSTANCE;
    }

    private ProducerService() {
    }

    public KafkaProducer<String, String> getProducer() {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Constant.MESSAGE_SIZE);
        return new KafkaProducer<>(properties);
    }


    public List<String> getUsZip() {
        CSVReader reader = null;
        List<String> zips = new ArrayList<>();
        try {
            InputStream is = ProducerDemo.class.getResourceAsStream(Constant.DATA_TEST_SOURCES);
            if (is == null) {
                throw new IOException();
            }
            reader = new CSVReader(new InputStreamReader(is));
            String[] line;
            while ((line = reader.readNext()) != null) {
                String code = line[0];
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

//    public List<List<String>> chunkByFiles(List<String> list, int n) {
//        List<List<String>> chunks = new ArrayList<>();
//
//        int size = list.size();
//        int chunkSize = (int) Math.ceil((double) size / n);
//
//        for (int i = 0; i < size; i += chunkSize) {
//            int end = Math.min(size, i + chunkSize);
//            chunks.add(list.subList(i, end));
//        }
//
//        return chunks;
//    }

    public List<List<String>> chunkBySize(List<String> list, int n) {
        List<List<String>> chunks = new ArrayList<>();

        for (int i = 0; i < list.size(); i += n) {
            int end = Math.min(list.size(), i + n);
            chunks.add(new ArrayList<>(list.subList(i, end)));
        }

        return chunks;
    }

    public Weather getWeatherData(String zipcode) {
        Request request = new Request.Builder()
                .url("https://weatherapi-com.p.rapidapi.com/current.json?q=" + zipcode)
                .get()
                .addHeader("X-RapidAPI-Key", "35ae57a75dmshfa432d963090737p1251cfjsn78a4b5dbd63a")
                .addHeader("X-RapidAPI-Host", "weatherapi-com.p.rapidapi.com")
                .build();
        Response response = null;
        if (invalidZipcodes.contains(zipcode)) {
            return null;
        }
        try {
            response = client.newCall(request).execute();
            if (response.code() == 200) {
                String body = Objects.requireNonNull(response.body()).string();
                WeatherData dto = gson.fromJson(body, WeatherData.class);
                return new Weather(zipcode, dto, simpleDateFormat.format(new Date()));
            } else {
                invalidZipcodes.add(zipcode);
                log.warn("GET Weather data by zip " + zipcode + " " + Objects.requireNonNull(response.body()).string());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return null;
    }

    public void publishData(KafkaProducer<String, String> producer, ProducerRecord<String, String> producerRecord) {

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
