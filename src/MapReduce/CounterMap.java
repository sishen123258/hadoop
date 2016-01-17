package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by root on 12/19/15.
 */
public class CounterMap extends Configured implements Tool {


    enum LogCounter{
        LOGCOUNTRT
    }

    public static class LogStatics extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(LogCounter.LOGCOUNTRT).increment(1);
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job;
        return 0;
    }
}
