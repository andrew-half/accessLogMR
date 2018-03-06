package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import com.epam.apolulyakh.writable.TrafficStatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static com.epam.apolulyakh.IpTrafficReducer.COUNTER_GROUP;

@RunWith(JUnit4.class)
public class IpAggregatorMRTest {

    private Mapper mapper = new AccessLogMapper();
    private Reducer combiner = new IpTrafficCombiner();
    private Reducer reducer = new IpTrafficReducer();
    private MapReduceDriver<LongWritable, Text, Text, TrafficAggeregatedWritable, Text, TrafficStatWritable> mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    @Test
    public void testMapReduceBasic() throws IOException {
        mapReduceDriver.getConfiguration().set("foo","bar");
        mapReduceDriver.withMapper(mapper);
        mapReduceDriver.withCombiner(combiner);
        mapReduceDriver.withReducer(reducer);

        mapReduceDriver.withInput(new LongWritable(0),
                    new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withOutput(new Pair<>(new Text("ip1"), new TrafficStatWritable(Double.valueOf(40028), Long.valueOf(40028))));
        mapReduceDriver.withCounter(COUNTER_GROUP, "Robot/Spider", 1);
        mapReduceDriver.runTest();
    }
}

