package miu.edu.bdt.mapreduce.lab.extra3.solution;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StationTempWritable implements WritableComparable<StationTempWritable> {

    private String stationId;
    private float temp;

    public StationTempWritable() {
    }

    public StationTempWritable(String stationId, float temp) {
        this.stationId = stationId;
        this.temp = temp;
    }

    @Override
    public int compareTo(StationTempWritable obj) {
        if(obj.getClass() != this.getClass()) throw new IllegalArgumentException();
        if(Objects.equals(this.stationId, obj.getStationId())){
            return this.temp > obj.getTemp() ? -1 : 1;
        }
        else return this.stationId.compareTo(obj.getStationId());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj.getClass() != this.getClass()) return false;
        StationTempWritable o = (StationTempWritable) obj;
        return Objects.equals(this.stationId, o.getStationId()) && this.temp == o.getTemp();
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, temp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stationId);
        out.writeFloat(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId = in.readUTF();
        temp = in.readFloat();
    }

    @Override
    public String toString() {
        return this.stationId + "\t" + this.temp;
    }

    public String getStationId() {
        return stationId;
    }

    public float getTemp() {
        return temp;
    }
}
