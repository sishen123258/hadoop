package domain.split;

import domain.minmax.MinUserIdMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.CORBA.Object;

import java.io.IOException;
import java.util.Map;

/**
 * 链接两个MapReduce
 */
public class SplitSearchData {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args=new String[]{"/tong/sogo", "/tong/sogo/split1","/tong/sogo/split2"};

        Configuration conf=new Configuration();
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/yarn-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/root/IdeaProjects/hadoop/conf/mapred-site.xml"));
        conf.set("mapred.jar","");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job1=new Job();
        job1.setJarByClass(SplitSearchData.class);
        job1.setMapperClass(SplitSearchMapper1.class);
        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1,new Path(otherArgs[1]));

        Job job2=new Job();
        job2.setJarByClass(SplitSearchData.class);
        job2.setMapperClass(SplitSearchMapper2.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job2,new Path(otherArgs[3]));

        System.exit(job1.waitForCompletion(true)? 0 : 1);
        System.exit(job2.waitForCompletion(true)? 0 : 1);


    }

    public static class SplitSearchMapper1 extends Mapper<Text,Text,Text,NullWritable>{

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Map logMap = MinUserIdMapper.parseLogToMap(value.toString());
            long lineNum = Long.parseLong(key.toString());
            String text =lineNum+ "\t" + logMap.get("hour") + "\t" + logMap.get("userId");
            context.write(new Text(text),NullWritable.get());
        }
    }

    public static class SplitSearchMapper2 extends Mapper<Text,Text,Text,NullWritable>{

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Map logMap = MinUserIdMapper.parseLogToMap(value.toString());
            long lineNum = Long.parseLong(key.toString());
            String text =lineNum+ "\t" + logMap.get("userId") + "\t" + logMap.get("searchWord")+
                    "\t" + logMap.get("pageOrder")+ "\t" + logMap.get("clickOrder")+ "\t" + logMap.get("url");
            context.write(new Text(text),NullWritable.get());
        }
    }



}
