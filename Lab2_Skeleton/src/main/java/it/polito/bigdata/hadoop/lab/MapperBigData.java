package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 

            String prefix = context.getConfiguration().get("filtering_string");

            if(key.toString().toLowerCase().startsWith(prefix)){
                context.getCounter(DriverBigData.MY_COUNTER.PASSED).increment(1);
                String single_word = key.toString().toLowerCase();
                context.write(new Text(single_word), new IntWritable(new Integer (value.toString())));
            }else{
                context.getCounter(DriverBigData.MY_COUNTER.NOT_PASSED).increment(1);
            }

    }
}
