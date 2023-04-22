package miu.edu.bdt.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Weather {

    private String zipcode;
    private String city;
    private float temp;
    private String updatedDate;

}
