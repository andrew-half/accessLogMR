package com.epam.apolulyakh.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrafficAggeregatedWritable implements Writable {
    private Long numRequests = Long.valueOf(0);
    private Long sum = Long.valueOf(0);
    private String agents = "";

    public TrafficAggeregatedWritable() {
    }

    public TrafficAggeregatedWritable(Long numRequests, Long sum, String agents) {
        this.numRequests = numRequests;
        this.sum = sum;
        this.agents = agents;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(numRequests);
        dataOutput.writeLong(sum);
        dataOutput.writeUTF(agents);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numRequests = dataInput.readLong();
        sum = dataInput.readLong();
        agents = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "" + numRequests + " " + sum + " " + agents;
    }

    @Override
    public int hashCode() {
        int hash = 37;
        hash = hash * 17 + numRequests.hashCode();
        hash = hash * 17 + sum.hashCode();
        hash = hash * 17 + agents.hashCode();
        return hash;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TrafficAggeregatedWritable)) {
            return false;
        }

        TrafficAggeregatedWritable another = (TrafficAggeregatedWritable) o;

        return ((numRequests == another.numRequests) || ((numRequests != null) && numRequests.equals(another.numRequests)))
                && ((sum == another.sum) || ((sum != null) && (sum.equals(another.sum))) )
                && ((agents == another.agents) || (agents != null && agents.equals(another.agents)));
    }

    public long getNumRequests() {
        return numRequests;
    }

    public void setNumRequests(long numRequests) {
        this.numRequests = numRequests;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public String getAgents() {
        return agents;
    }

    public void setAgents(String agents) {
        this.agents = agents;
    }
}
