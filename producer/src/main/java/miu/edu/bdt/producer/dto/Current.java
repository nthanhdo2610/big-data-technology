package miu.edu.bdt.producer.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class Current {
    private long last_updated_epoch;
    private String last_updated;
    private float temp_c;
    private float temp_f;
    private int is_day;
    private Condition condition;
    private float wind_mph;
    private float wind_kph;
    private float wind_degree;
    private String wind_dir;
    private float pressure_mb;
    private float pressure_in;
    private float precip_mm;
    private float precip_in;
    private float humidity;
    private float cloud;
    private float feelslike_c;
    private float feelslike_f;
    private float vis_km;
    private float vis_miles;
    private float uv;
    private float gust_mph;
    private float gust_kph;

}
