package domain.minmax;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MinUserIdMapper extends Mapper<Object,Text,LongWritable,NullWritable>{

        public long min=Long.MAX_VALUE;


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String,String> userIdMap= parseLogToMap(value.toString());

            String pageOrder = userIdMap.get("pageOrder");
            try {
                long userId = Long.parseLong(pageOrder);
                if (userId < min) {
                    min = userId;
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(min),NullWritable.get());
        }

        public static Map parseLogToMap(String s) {

            String[] dataList=s.toString().split("\t");
            Map userLogMap=new HashMap<String,String>();

            userLogMap.put("hour",dataList[0]);
            userLogMap.put("userId",dataList[1]);
            userLogMap.put("searchWord",dataList[2]);
            userLogMap.put("pageOrder",dataList[3]);
            userLogMap.put("clickOrder",dataList[4]);
            userLogMap.put("url",dataList[5]);
            return userLogMap;
        }
    }