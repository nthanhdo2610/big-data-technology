/**
 * @author Nhat Pham - 986847
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StationTempRecordInYear implements WritableComparable<StationTempRecordInYear> {	
	private long stationId = 0;
	private long temp = 0;
	
	public long getTemp() {
		return temp;
	}
	
	public void setTemp(long temp) {
		this.temp = temp;
	}

	public long getStationId() {
		return stationId;
	}

	public void setStationId(long stationId) {
		this.stationId = stationId;
	}	
	
	public void readFields(DataInput in) throws IOException {
		stationId = in.readLong();
		temp = in.readLong();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(stationId);
		out.writeLong(temp);
	}
	
	@Override
	public String toString() {
		String str = String.valueOf(this.stationId);
		return String.format("%s-%s %5s", str.substring(0, 5), str.substring(5), this.temp);		
	}	

	@Override
	public int compareTo(StationTempRecordInYear obj) {
		if (this.stationId == obj.stationId) {
			if (this.temp > obj.temp) return -1;
			if (this.temp == obj.temp) return 0;
			if (this.temp < obj.temp) return 1;
		} 
		return this.stationId < obj.stationId ? -1 : 1;
	}
}