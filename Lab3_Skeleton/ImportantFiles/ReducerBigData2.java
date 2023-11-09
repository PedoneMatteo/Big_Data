package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    TopKVector<WordCountWritable> top3;

    protected void setup(Context context) {
        top3 = new TopKVector<WordCountWritable>(100);
    }

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        top3.updateWithNewElement(new WordCountWritable(key.toString(), new Integer(sum)));

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Vector<WordCountWritable> top3Objects = top3.getLocalTopK();
        for (WordCountWritable pair : top3Objects) {
            context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
        }
    }
}
