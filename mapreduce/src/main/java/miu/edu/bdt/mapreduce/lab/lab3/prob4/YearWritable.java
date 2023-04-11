package miu.edu.bdt.mapreduce.lab.lab3.prob4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearWritable implements WritableComparable<YearWritable> {
    private int year;

    public YearWritable() {
    }

    public YearWritable(int year) {
        this.year = year;
    }

    @Override
    public int compareTo(YearWritable obj) {
        if(obj.getClass() != this.getClass()) throw new IllegalArgumentException();
        if(this.year == obj.getYear()) return 0;
        else return this.year > obj.getYear() ? -1 : 1;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj.getClass() != this.getClass()) return false;
        YearWritable anotherYear = (YearWritable) obj;
        return this.year == anotherYear.getYear();
    }

    @Override
    public int hashCode() {
        return Objects.hash(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
    }

    public int getYear() {
        return year;
    }
}
