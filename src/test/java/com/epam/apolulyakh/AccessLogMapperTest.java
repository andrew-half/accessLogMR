package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
@RunWith(JUnit4.class)
public class AccessLogMapperTest {

    AccessLogMapper mapper;
    Mapper.Context context;
    MapDriver<LongWritable, Text, Text, TrafficAggeregatedWritable> mapDriver;

    private static List getInput(String filePath) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(AccessLogMapperTest.class.getClassLoader().getResource(filePath).getFile()));
        List input = new ArrayList();
        while(scanner.hasNext()){
            String line = scanner.nextLine();
            input.add(new Pair(new LongWritable(0), new Text(line)));
        }
        return input;
    }

    @Before
    public void setup() {
        mapper = new AccessLogMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testDifferentLength() throws IOException, InterruptedException {
        mapDriver.addAll(getInput("access_logs/input/000000"));
    }


}
