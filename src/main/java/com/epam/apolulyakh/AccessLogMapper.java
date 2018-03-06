package com.epam.apolulyakh;

import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessLogMapper extends Mapper<LongWritable, Text, Text, TrafficAggeregatedWritable> {

    private static class FIELDS {
        static int IP = 0;
        static int UNUSED1 = 1;
        static int USERID = 2;
        static int TIME_TIME = 3;
        static int TIME_TIMEZONE = 4;
        static int HTTP_METHOD = 5;
        static int HTTP_RESOURCE = 6;
        static int HTTP_PROTOCOL = 7;
        static int HTTP_STATUS = 8;
        static int SIZE = 9;
        static int REFERRER = 10;
        static int USER_AGENT = 11;
    }

    private TrafficAggeregatedWritable aggrValue = new TrafficAggeregatedWritable();
    private Text ip = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ", 12);
        if (fields.length < 12) {
            System.out.println("Broken line " + line);
            return;
        }

        try {
            ip.set(fields[FIELDS.IP]);
            aggrValue.setNumRequests(1);
            aggrValue.setSum("-".equals(fields[FIELDS.SIZE]) ? 0 : Long.valueOf(fields[FIELDS.SIZE]));
            aggrValue.setAgents(UserAgent.parseUserAgentString(fields[FIELDS.USER_AGENT]).getBrowser().getName());

            context.write(ip, aggrValue);
        } catch (Exception ex) {
            System.out.println("Broken line " + line);
        }
    }
}
