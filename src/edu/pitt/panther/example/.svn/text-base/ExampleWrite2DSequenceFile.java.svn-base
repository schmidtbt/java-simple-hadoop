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
 * Time: 11:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExampleWrite2DSequenceFile {

    private static final double[][] DATA = {
            {0.0, 0.1, 0.2},
            {0.0, 0.1, 0.2},
            {0.0, 0.1, 0.2},
            {0.0, 0.1, 0.2},
            {0.0, 0.1, 0.2},
            {0.0, 0.1, 0.2},
    };

    public static void main(String[] args) throws IOException {
        // Get command line argument 1 as the new file to create
        String uri = args[0];
        Configuration conf = new Configuration();
        // Create this new file if it does not exist
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);


        IntWritable key = new IntWritable();
        DoubleArrayWritable value = new DoubleArrayWritable();


        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path,
                    key.getClass(), value.getClass());

            for (int i = 0; i < DATA.length; i++) {
                
                DoubleWritable[] arr = new DoubleWritable[DATA[i].length];
                
                for( int j = 0; j < DATA[i].length; j++ ){
                    System.out.println( j );
                    System.out.println( i );
                    System.out.println( DATA[i][j] );
                    System.out.println( arr[j] );
                    arr[j] = new DoubleWritable();
                    arr[j].set( DATA[i][j] );
                }
                
                key.set(i);
                value.set( arr );
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}


