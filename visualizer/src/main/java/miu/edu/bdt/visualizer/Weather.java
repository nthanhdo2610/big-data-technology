package miu.edu.bdt.visualizer;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Weather {

	private String zipcode;
	private String city;
	private float temp;
	private String updatedDate;

}
