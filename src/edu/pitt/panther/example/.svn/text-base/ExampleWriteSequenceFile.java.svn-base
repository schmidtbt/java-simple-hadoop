package edu.pitt.panther.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 11:00 AM
 * Going to attempt to write a sequence file of doubles.
 *
 * This file can be read by using:
 * hadoop fs -text output.seq
 */
public class ExampleWriteSequenceFile {

    private static final double[] DATA = {
            0.0,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5
    };

    public static void main(String[] args) throws IOException {
        // Get command line argument 1 as the new file to create
        String uri = args[0];
        Configuration conf = new Configuration();
        // Create this new file if it does not exist
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);


        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path,
                    key.getClass(), value.getClass());

            for (int i = 0; i < DATA.length; i++) {
                key.set(i);
                value.set(DATA[i]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
