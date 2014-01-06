package edu.pitt.panther.preprocess;

import edu.pitt.panther.example.DoubleArrayWritable;
import org.apache.commons.math.distribution.TDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/22/12
 * Time: 10:25 AM
 *
 * Take in a 4D binary file (for now, outputted from matlab column oriented) and save as a sequence file
 * in either a by Slice or by Time (for a voxel) with the INT key being the column-keyed index of that
 * slice or that time-course.
 *
 * Usage:
 * Xdim Ydim Tdim inputBinaryFile
 *
 */
public class ConvertImageToSequence {

    public static void main( String[] args ) throws IOException {

        // Get the input parameters
        int Xdim, Ydim, Tdim;
        Xdim = Integer.parseInt( args[0] );
        Ydim = Integer.parseInt( args[1] );
        Tdim = Integer.parseInt( args[2] );

        String uri = new String( args[3] );
        String output = new String( args[4] );

        byTime( Xdim, Ydim, Tdim, uri, output);

    }

    
    public static int[][] readArray( int Xdim, int Ydim, int Tdim, String binaryFile ){
        
        int[][] output = new int[Xdim][Ydim*Tdim];
        return output;

    }

    public static void byTime( int Xdim, int Ydim, int Tdim, String binaryFile, String outputseq ) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(outputseq), conf);
        Path path = new Path(outputseq);


        IntWritable key = new IntWritable();
        DoubleArrayWritable value = new DoubleArrayWritable();

        SequenceFile.Writer writer = null;

        try {
            // Create the input file reader
            FileInputStream in = new FileInputStream( binaryFile );
            int currentValue;
            int xCount = 0;
            int yCount = 0;
            int counter = 0;
            // Create one giant array to store all data values
            int totalValues = Xdim*Ydim*Tdim;
            double[] input = new double[ totalValues ];
            int valuesInSlice = Xdim*Ydim;

            // Create teh sequence file output writer
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());


            // Populate the Input array -- for time, we must read all values first (with this method)
            while ((currentValue = in.read()) != -1) {

                // Add the value to the running total
                input[counter] = currentValue;

                // Progress reporting
                if( counter % 10000 == 0 ){
                    Double percent = new Double( (double)counter / (double)totalValues );
                    System.out.println( percent.toString() );
                }

                counter++;
            }

            DoubleWritable[] timeseries = new DoubleWritable[ Tdim ];
            DoubleWritable val = new DoubleWritable(0);

            // Loop over each voxel
            for( int i = 0; i < valuesInSlice; i++ ){

                // Now loop over all the time-values for this voxel
                for( int j = 0; j < Tdim; j++ ){
                    //val.set( input[i+(j*valuesInSlice)] );
                    timeseries[j] = new DoubleWritable();
                    timeseries[j].set( input[i+(j*valuesInSlice)] );
                }

                System.out.println();
                // Now write those values into the sequence file
                System.out.println( "Writing key: " + i );
                key.set( i );
                value.set( timeseries );
                writer.append(key, value);
            }

        } finally {
            IOUtils.closeStream(writer);
        }


    }
    
}
