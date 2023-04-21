package miu.edu.bdt.consumer.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class WeatherData {
    private String zipcode;
    private Location location;
    private Current current;
}
