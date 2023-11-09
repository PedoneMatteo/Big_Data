package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    TopKVector<WordCountWritable> top3;

    protected void setup(Context context) {
        top3 = new TopKVector<WordCountWritable>(100);
    }

    protected void map(
            Text key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {
        /* Implement the map method */
        top3.updateWithNewElement(new WordCountWritable(key.toString(), new Integer(value.toString())));

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Vector<WordCountWritable> top3Objects = top3.getLocalTopK();
        for (WordCountWritable pair : top3Objects) {
            context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
        }
    }

}
