package miu.edu.bdt.producer.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Weather {

    private String zipcode;
    private float temp;

    public Weather(String zipcode, WeatherData dto) {
        this.zipcode = zipcode;
        this.temp = dto.getCurrent().getTemp_f();
    }
}
