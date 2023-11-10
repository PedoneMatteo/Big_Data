package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        FloatWritable> { // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */
        float sum = 0;
        int cont = 0;
        /*crea una lista vuota*/
        List<String> list = new ArrayList<String>();

        for (Text value : values) {
            String fields[] = value.toString().split(",");
            String productId = fields[0];
            String score = fields[1];

            list.add(productId+","+score);
            sum += Float.parseFloat(score);
            cont++;
        }

        float meanValue = (float) sum / cont;

        for (String value : list) {
            String fields[] = value.toString().split(",");
            String productId = fields[0];
            String score = fields[1];

            Float NormalizedValue = Float.parseFloat(score) - meanValue;
            context.write(new Text(productId), new FloatWritable(NormalizedValue));
        }

    }
}
