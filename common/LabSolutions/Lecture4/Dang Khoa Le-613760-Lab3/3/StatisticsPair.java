import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatisticsPair implements Writable{
    private double sum;
    private int count;

    StatisticsPair(){}

    public StatisticsPair(double sum,  int count){
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeDouble(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        sum = in.readDouble();
    }

    public double getSum(){
        return this.sum;
    }

    public int getCount(){
        return this.count;
    }
    
}
