package edu.pitt.panther.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 11:09 AM
 * Read back a sequence file of INT -> Double values
 *
 * This Reads the sequence file type from the sequence file itself
 */
public class ExampleReadSequenceFile {

    public static void main(String[] args) throws IOException {
        // Get path, and open file from command line argument 1
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), conf);
            System.out.println( reader.getValueClass() );
            System.out.println( reader.getKeyClass() );
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value );
                position = reader.getPosition(); // beginning of next record
                DoubleArrayWritable daw = (DoubleArrayWritable)value;
                Writable[] dd = (Writable[])daw.get();
                for( int i = 0; i < dd.length; i++ ){
                    DoubleWritable dw = (DoubleWritable)dd[i];
                    System.out.print( dw.get() );
                }
                System.out.println();

            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

}
