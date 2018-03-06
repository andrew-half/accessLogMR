package com.epam.apolulyakh.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrafficStatWritable implements Writable {

    public static final double PRECISION  = 0.1;
    private Double average = 0.0;
    private Long total = 0L;

    public TrafficStatWritable() {}

    public TrafficStatWritable(Double average, Long total) {
        this.average = average;
        this.total = total;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(average);
        dataOutput.writeLong(total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        average = dataInput.readDouble();
        total = dataInput.readLong();
    }

    @Override
    public int hashCode() {
        int hash = 37;
        hash = hash * 17 + total.hashCode();
        hash = hash * 17 + average.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TrafficStatWritable)) {
            return false;
        }

        TrafficStatWritable another = (TrafficStatWritable) o;
        return ((total == another.total) || ((total != null) && total.equals(another.total)))
                && ((average == another.average)
                    || (average != null && another.average != null && (Math.abs(average-another.average) < PRECISION)));
    }

    @Override
    public String toString() {
        return "" + String.format("%.1f",average) + ", " + total;
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }
}
