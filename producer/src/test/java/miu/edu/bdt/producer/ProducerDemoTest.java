package miu.edu.bdt.producer;

import java.io.IOException;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

class ProducerDemoTest {

    private static final OkHttpClient client = new OkHttpClient();

    @Test
    void getWeatherData() throws IOException {
        Request request = new Request.Builder()
                .url("https://weatherapi-com.p.rapidapi.com/current.json?q=" + 52556)
                .get()
                .addHeader("X-RapidAPI-Key", "35ae57a75dmshfa432d963090737p1251cfjsn78a4b5dbd63a")
                .addHeader("X-RapidAPI-Host", "weatherapi-com.p.rapidapi.com")
                .build();
        Response response = client.newCall(request).execute();
        System.out.println(Objects.requireNonNull(response.body()).string());
    }
}