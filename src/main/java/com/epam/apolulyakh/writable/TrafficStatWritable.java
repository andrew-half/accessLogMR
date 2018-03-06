package com.epam.apolulyakh.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrafficStatWritable implements Writable {

    public static final double PRECISION  = 0.1;
    private Double average = new Double(0);
    private Long total = new Long(0);

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
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TrafficStatWritable)) {
            return false;
        }

        TrafficStatWritable another = (TrafficStatWritable) o;
        return (total.equals(another.getTotal()) && (Math.abs(total-another.total) < PRECISION));
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
