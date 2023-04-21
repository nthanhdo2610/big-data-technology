package miu.edu.bdt.producer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import miu.edu.bdt.producer.dto.Weather;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class ProducerServiceTest {

    private static final ProducerService service = new ProducerService();
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static final Gson gson = new Gson();

    @Test
    void getWeatherData() {

        Set<String> set = service.getUsZip();
        //create a list to hold the Future object associated with Callable
        List<Future<Weather>> dataFutures = new ArrayList<>();

        List<Weather> weathers = new ArrayList<>();

        for (String zip : set) {

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
        System.out.println(gson.toJson(weathers, new TypeToken<List<Weather>>() {}.getType()));
    }
}