package com.epam.apolulyakh;


import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import com.epam.apolulyakh.writable.TrafficStatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;

@RunWith(JUnit4.class)
public class IpTrafficReducerTest {

    IpTrafficReducer reducer = new IpTrafficReducer();
    ReduceDriver<Text, TrafficAggeregatedWritable, Text, TrafficStatWritable> reducerDriver;

    @Before
    public void setup() {
        reducerDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduce() throws IOException {
        reducerDriver.withInput(new Text("ip1"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(5L, 300L, "agent1"))
        );

        reducerDriver.withOutput(new Text("ip1"),
                new TrafficStatWritable(66.7, 400L));
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent1", 1);
        reducerDriver.runTest();
    }

    @Test
    public void testReduceDifferentAgents() throws IOException {
        reducerDriver.withInput(new Text("ip1"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 50L, "agent1"),
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(3L, 300L, "agent2"))
        );

        reducerDriver.withOutput(new Text("ip1"),
                new TrafficStatWritable(90.0, 450L));
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent1", 1);
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent2", 1);
        reducerDriver.runTest();
    }

    @Test
    public void testReduceDifferentAgentsDifferentIp() throws IOException {
        reducerDriver.withInput(new Text("ip1"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(2L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(3L, 300L, "agent2"))
        );

        reducerDriver.withInput(new Text("ip2"),
                Arrays.asList(
                        new TrafficAggeregatedWritable(1L, 100L, "agent1"),
                        new TrafficAggeregatedWritable(1L, 300L, "agent3"))
        );
        reducerDriver.withOutput(new Text("ip1"),
                new TrafficStatWritable(83.3, 500L));
        reducerDriver.withOutput(new Text("ip2"),
                new TrafficStatWritable(200.0, 400L));
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent1", 2);
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent2", 1);
        reducerDriver.withCounter(IpTrafficReducer.COUNTER_GROUP, "agent3", 1);
        reducerDriver.runTest();
    }
}
