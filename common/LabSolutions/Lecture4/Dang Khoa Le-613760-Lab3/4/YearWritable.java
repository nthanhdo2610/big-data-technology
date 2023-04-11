import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class YearWritable implements WritableComparable{
    private int year;
    
    public YearWritable(int year) {
        this.year = year;
    }

    YearWritable(){}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       year = in.readInt(); 
    }

    @Override
    public int compareTo(Object obj) {
        if(obj.getClass() != this.getClass()) throw new IllegalArgumentException();
        YearWritable anotherYear = (YearWritable) obj;
        if(this.year == anotherYear.get()) return 0;
        else return this.year > anotherYear.get() ? -1 : 1;
    }

    public int get(){
        return year;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj.getClass() != this.getClass()) return false;
        YearWritable anotherYear = (YearWritable) obj; 
        return this.year == anotherYear.get();
    }

    @Override
    public int hashCode() {
        return 
        Objects.hash(year);    
    }
    
}
