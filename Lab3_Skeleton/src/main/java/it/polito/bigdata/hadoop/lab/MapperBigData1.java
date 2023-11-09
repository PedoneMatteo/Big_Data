package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
                Text, // Input value type
                Text, // Output key type
                IntWritable> {// Output value type

        protected void map(
                        LongWritable key, // Input key type
                        Text value, // Input value type
                        Context context) throws IOException, InterruptedException {

                /* Implement the map method */
                String fields[] = value.toString().split(",");
                int len = fields.length;
                int i, j;

                for (i = 1; i < len; i++) {
                        for (j = i + 1; j < len; j++) {
                                String product1 = fields[i];
                                String product2 = fields[j];
                                String pair;
                                if(product1.compareTo(product2)<0)
                                        pair = product1 + "," + product2;
                                else 
                                        pair = product2 + "," + product1;
                                context.write(new Text(pair), new IntWritable(1));
                        }
                }

        }

}
