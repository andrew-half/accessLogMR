package com.epam.apolulyakh;


import com.epam.apolulyakh.writable.TrafficAggeregatedWritable;
import com.epam.apolulyakh.writable.TrafficStatWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class IpAggregatorMR extends Configured implements Tool {


    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run( new Configuration(), new IpAggregatorMR(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ipaggregator <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "access log stat");
        job.setJarByClass(IpAggregatorMR.class);

        job.setMapperClass(AccessLogMapper.class);
        job.setCombinerClass(IpTrafficCombiner.class);
        job.setReducerClass(IpTrafficReducer.class);

        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficAggeregatedWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficStatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        if (job.waitForCompletion(true)) {

            for (Counter counter : job.getCounters().getGroup(IpTrafficReducer.COUNTER_GROUP)) {
                System.out.println(counter.getName() + " used " + counter.getValue());
            }
            return 0;
        } else {
            return 1;
        }

    }
}
