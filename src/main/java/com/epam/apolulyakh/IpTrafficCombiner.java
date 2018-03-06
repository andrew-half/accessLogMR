package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

public class IpTrafficCombiner extends Reducer<Text, TrafficAggeregatedWritable, Text, TrafficAggeregatedWritable> {

        private TrafficAggeregatedWritable aggregatedWritable = new TrafficAggeregatedWritable();

        @Override
        protected void reduce(Text ip, Iterable<TrafficAggeregatedWritable> traffic, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long requestCount = 0;
            Set<String> agents = new HashSet<>();
            for (TrafficAggeregatedWritable value : traffic) {
                sum += value.getSum();
                requestCount += value.getNumRequests();
                agents.add(value.getAgents());
            }

            StringJoiner joiner = new StringJoiner(",");
            for (String agent : agents) {
                joiner.add(agent);
            }

            aggregatedWritable.setSum(sum);
            aggregatedWritable.setNumRequests(requestCount);
            aggregatedWritable.setAgents(joiner.toString());
            context.write(ip, aggregatedWritable);
        }
    }
