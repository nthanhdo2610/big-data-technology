package miu.edu.bdt.lab.lab3.prob5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatIntPairWritable implements Writable {

    private float sum;
    private int count;

    public FloatIntPairWritable() {
    }

    public FloatIntPairWritable(float sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeFloat(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        sum = in.readFloat();
    }

    public float getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }
}
