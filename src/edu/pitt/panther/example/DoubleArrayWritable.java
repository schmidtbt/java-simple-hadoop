package edu.pitt.panther.example;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 11:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable(){
        super(DoubleWritable.class);
    }
}
