package domain.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Tong on 2016/1/19.
 */
public class PartitionMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"/tong/sogo", "/tong/sougo/partition"};

        Configuration conf=new Configuration();
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/yarn-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/mapred-site.xml"));
        conf.set("mapred.jar","/root/IdeaProjects/hadoop/out/artifacts/minSogouUserId_jar/hadoop.jar");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job=new Job(conf,"word count");
        job.setJarByClass(PartitionMR.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
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

    public static class PartitionReduce extends Reducer<IntWritable,Text,Text,NullWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text: values){
                //context.write 每执行一次就是一条记录写到文件中
                context.write(text,NullWritable.get());
            }

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
