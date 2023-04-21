package miu.edu.bdt.consumer.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import miu.edu.bdt.consumer.dto.WeatherData;

@Getter
@Setter
@NoArgsConstructor
public class Weather {

    private String zipcode;
    private float temp;

    public Weather(WeatherData dto) {
        this.zipcode = dto.getZipcode();
        this.temp = dto.getCurrent().getTemp_f();
    }
}
