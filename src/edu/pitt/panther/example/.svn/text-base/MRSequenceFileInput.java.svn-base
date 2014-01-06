package edu.pitt.panther.example;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 11:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class MRSequenceFileInput extends Configured implements Tool {


    enum MyCounter {
        GOOD,
        BAD
    }

    public static class Map extends MapReduceBase implements
            Mapper<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(IntWritable key, DoubleWritable value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws
                IOException {

            output.collect( key , value );

        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterator<DoubleWritable> values,
                           OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws
                IOException {

            DescriptiveStatistics stats = new DescriptiveStatistics();

            while (values.hasNext()) {
                stats.addValue(values.next().get());
            }

            double mean = stats.getMean();
            double std = stats.getStandardDeviation();
            double median = stats.getPercentile(50);

            System.out.println( "Mean: " + mean );
            System.out.println( "std: " + std );
            System.out.println( "median: " + median );
            output.collect(key, new DoubleWritable(mean));
        }
    }

    public int run( String[] args ) throws Exception {
        if( args.length != 2 ){
            System.err.printf( "Usage: %s [ generic options ] <input> <output>\n" +
                    "Expects the input directory path and an output directory path. The output directory\n" +
                    "cannot exist\n",
                    getClass().getSimpleName() );
            ToolRunner.printGenericCommandUsage(System.err);

        }
        JobConf conf = new JobConf(MRSequenceFileInput.class);
        conf.setJobName("MRSequenceFileInput");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }


    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run( new MRSequenceFileInput(), args );
        System.exit(exitCode);

    }


}
