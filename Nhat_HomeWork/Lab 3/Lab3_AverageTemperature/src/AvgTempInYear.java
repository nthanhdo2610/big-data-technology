/**
 * @author Nhat Pham - 986847
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AvgTempInYear implements Writable {
	private long sum = 0;
	private long count = 0;
	
	public long getSum() {
		return sum;
	}
	
	public void setSum(long sum) {
		this.sum += sum;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count += count;
	}
	
	public void readFields(DataInput in) throws IOException {
		sum = in.readLong();
		count = in.readLong();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(sum);
		out.writeLong(count);
	}
}