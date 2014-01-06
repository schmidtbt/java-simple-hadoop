package edu.pitt.panther.example;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: bschmidt
 * Date: 2/21/12
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 *
 * Files are normally column oriented. That is, they go down (X-direction) rows for a given column
 * then increment the column (move to right, Y-direction)
 *
 *
 *
 */
public class ReadBinFile {

    public static void main(String[] args)
            throws IOException {


        int Xsize = 384;
        int Ysize = 55296;

        double[][] img = new double[Xsize][Ysize];

        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(args[0]);
            out = new FileOutputStream(args[1]);
            int c;

            int xCount = 0;
            int yCount = 0;

            while ((c = in.read()) != -1) {
                //System.out.println( c );
                out.write(c);

                img[xCount][yCount] = (double)c;

                xCount++;
                if( xCount >= Xsize ){
                    if( yCount % 10 == 0 ){
                        Double percent = new Double( (double)yCount / (double)Ysize );
                        System.out.println("Completed: " + percent.toString() );
                    }

                    xCount = 0;
                    yCount++;
                    if( yCount >= Ysize ){
                        break;
                    }
                }

            }
            
            printImg( img );

        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
        
    }

    /**
     * Print to stdout the 2D image of pixels as INT
     * @param img
     */
    public static void printImg( double[][] img ){
        
        for( int i = 0; i < img.length; i++ ){
            
            for( int j = 0; j < img.length; j++ ){

                System.out.print( (int)img[i][j] + " ");

            }
            System.out.println();
            
        }
        
        
    }
    
    
    
}
