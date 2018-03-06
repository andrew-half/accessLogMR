package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class AccessLogMapperTest {

    AccessLogMapper mapper;
    MapDriver<LongWritable, Text, Text, TrafficAggeregatedWritable> mapDriver;

    @Before
    public void setup() {
        mapper = new AccessLogMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testParseInputMozilla() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0)\""));
        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 40028L, "Mozilla"));
        mapDriver.runTest();
    }

    @Test
    public void testParseInputBot() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028 \"-\" \"Mozilla/5.0 (Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots))\""));
        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 40028L, "Robot/Spider"));
        mapDriver.runTest();
    }

    @Test
    public void testParseInputTooShort() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028"));
        mapDriver.runTest();
    }

    @Test
    public void testParseInputTooLong() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0)\" addSomething"));
        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 40028L, "Mozilla"));
        mapDriver.runTest();
    }

    @Test
    public void testParseNotNumberTraffic() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 foo \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0)\" addSomething"));
        mapDriver.runTest();
    }

    @Test
    public void testParseUnknownAgent() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028 \"-\" \"My nice browser\""));
        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 40028L, "Unknown"));
        mapDriver.runTest();
    }

    @Test
    public void testSeveralInputs() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] " +
                        "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
                        "200 40028 \"-\" \"Mozilla/5.0 (Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots))\""));
        mapDriver.withInput(new LongWritable(0),
                new Text("ip4 - - [24/Apr/2011:04:23:54 -0400] " +
                        "\"HEAD /sgi_indigo2/ HTTP/1.0\" " +
                        "200 0 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4)\""));
        mapDriver.withInput(new LongWritable(0),
                new Text("ip1 - - [24/Apr/2011:04:18:54 -0400] " +
                        "\"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" " +
                        "200 6244 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 40028L, "Robot/Spider"));
        mapDriver.withOutput(new Text("ip4"), new TrafficAggeregatedWritable(1L, 0L, "Internet Explorer 8"));
        mapDriver.withOutput(new Text("ip1"), new TrafficAggeregatedWritable(1L, 6244L, "Robot/Spider"));
        mapDriver.runTest();
    }
}
