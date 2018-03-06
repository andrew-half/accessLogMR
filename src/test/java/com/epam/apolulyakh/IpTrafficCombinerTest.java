package com.epam.apolulyakh;


import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;

@RunWith(JUnit4.class)
public class IpTrafficCombinerTest {

    IpTrafficCombiner combiner = new IpTrafficCombiner();
    ReduceDriver<Text, TrafficAggeregatedWritable, Text, TrafficAggeregatedWritable> combinerDriver;

    @Before
    public void setup() {
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombineSingleAgent() throws IOException {

        combinerDriver.withInput(new Text("ip1"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(1L, 300L, "agent1"))
        );

        combinerDriver.withOutput(new Text("ip1"),
                new TrafficAggeregatedWritable(2L, 400L, "agent1"));
        combinerDriver.runTest();
    }


    @Test
    public void testCombineSeveralAgents() throws IOException {
        combinerDriver.withInput( new Text("ip1"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(2L, 200L, "agent2"),
                        new TrafficAggeregatedWritable(1L, 300L, "agent1")));

        combinerDriver.withOutput(new Text("ip1"),
                new TrafficAggeregatedWritable(4L, 600L, "agent1,agent2"));

        combinerDriver.runTest();
    }


}
