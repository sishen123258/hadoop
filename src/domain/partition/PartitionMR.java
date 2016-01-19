package domain.partition;

import domain.minmax.MinUserIdMR;
import domain.minmax.MinUserIdMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Tong on 2016/1/19.
 */
public class PartitionMR {

    public static void main(String[] args) {

    }

    public static class PartitionMapper extends Mapper<Object,Text,IntWritable,Text>{

        private SimpleDateFormat simpleDateFormat=new SimpleDateFormat("HH:mm:ss");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String time=value.toString().substring(0,value.toString().indexOf("\t"));
            Calendar calendar=Calendar.getInstance();
            try {
                calendar.setTime(simpleDateFormat.parse(time));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            int hour=calendar.HOUR;
            context.write(new IntWritable(hour),value);
        }
    }

    /**
     * 实现自己的partition函数
     */
    public static class MyPartitioner extends Partitioner<IntWritable,Text>{

        /**
         * @param intWritable mapper output key
         * @param text mapper ouput value
         * @param i partition num,or reduce num
         * @return
         */
        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {


            return 0;

        }
    }


}
