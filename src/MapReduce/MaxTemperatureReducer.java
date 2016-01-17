package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 12/16/15.
 */
public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue=Integer.MIN_VALUE;

        for (IntWritable i : values){
            maxValue=Integer.max(maxValue,i.get());
        }
        context.write(key,new IntWritable(maxValue));
    }
}
