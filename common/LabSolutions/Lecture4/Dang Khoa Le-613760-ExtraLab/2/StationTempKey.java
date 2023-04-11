package extra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StationTempKey implements WritableComparable{
    private String station;
    private double temperature;

    StationTempKey(){}
    public StationTempKey(String station, double temp){
        this.setStation(station);
        this.setTemperature(temp);
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String statiion) {
        this.station = statiion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
       out.writeUTF(station); 
       out.writeDouble(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        station = in.readUTF();
        temperature = in.readDouble(); 
    }

    @Override
    public int compareTo(Object o) {
        if(o.getClass() != this.getClass()) throw new IllegalArgumentException();
        StationTempKey anotherKey = (StationTempKey) o;
        if(this.station.equals(anotherKey.getStation()) && this.temperature == anotherKey.getTemperature()) return 0;
        if(this.station.compareTo(anotherKey.getStation()) > 0 || 
            (this.station.compareTo(anotherKey.getStation()) == 0 && this.temperature < anotherKey.temperature)) return 1;
        return -1;
    }
    
}
