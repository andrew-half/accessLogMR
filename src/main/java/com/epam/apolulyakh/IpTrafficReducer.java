package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import com.epam.apolulyakh.writable.TrafficStatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IpTrafficReducer extends Reducer< Text, TrafficAggeregatedWritable, Text, TrafficStatWritable> {

    public static final String COUNTER_GROUP = "accesslog";

    private TrafficStatWritable statWritable = new TrafficStatWritable();
    private Set<String> agents = new HashSet<>();

    @Override
    protected void reduce(Text ip, Iterable<TrafficAggeregatedWritable> sizes, Context context) throws IOException, InterruptedException {


        long sum = 0;
        long requestNum = 0;
        agents.clear();

        for (TrafficAggeregatedWritable aggeregatedWritableValue : sizes) {
            sum += aggeregatedWritableValue.getSum();
            requestNum += aggeregatedWritableValue.getNumRequests();
            agents.addAll(Arrays.asList(aggeregatedWritableValue.getAgents().split(",")));
        }

        statWritable.setAverage(((double) sum / requestNum));
        statWritable.setTotal(sum);

        incrementAgentCounters(context);
        context.write(ip, statWritable);
    }

    private void incrementAgentCounters(Context context) {
        for (String agent : agents) {
            context.getCounter(COUNTER_GROUP, agent).increment(1);
        }
    }
}