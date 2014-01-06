package edu.pitt.panther.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 9:36 AM
 *
 * This example takes, line-by-line a series of text files and will find the number of 'Tokens' frequency outputting
 * the number of times that token was seen in a /t oriented list.
 *
 * Command line requires the input directory and output directory
 * java -jar out/artifacts/diss_jar/WordCount.jar examples/wordcount_input/ examples/wordcount_output/
 *
 * If the directory already exists, a:
 * Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/Users/bschmidt/Desktop/diss/examples/wordcount_output already exists
 * will be thrown by hadoop
 *
 */
public class WordCount {

    enum MyCounter {
        GOOD,
        BAD
    }

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws
                IOException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
                reporter.incrCounter(MyCounter.GOOD, 1);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws
                IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}


