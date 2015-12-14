package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Created by Yue on 2015/12/2.
 */
public class WordCount {


    public static class StringTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Logger logger = Logger.getLogger(StringTokenizerMapper.class.getName());
        private static final IntWritable one = new IntWritable(1);//这里缺少1出错
        private Text word = new Text();
        private int count = 0;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("logerloger " + count++ + " " + value.toString());
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
//                word.set(new Text(stringTokenizer.nextToken()));
                word.set(stringTokenizer.nextToken());
                logger.info("logerloger " + count + " " + word.toString());
                context.write(word, one);
            }
        }
    }

    public static class StringTokenizerReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        private Logger logger = Logger.getLogger(StringTokenizerReduce.class.getName());
        private IntWritable result=new IntWritable();
        private int count=0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("logerloger " + count++ + " " + key.toString());
            int sum=0;
            for (IntWritable v :values){
                sum+=v.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"/tong/input", "/output/wordcount4/"};

        Configuration conf=new Configuration();
        conf.addResource(new Path("D:\\web\\hadoop2\\conf\\core-site.xml"));
        conf.addResource(new Path("D:\\web\\hadoop2\\conf\\hdfs-site.xml"));
        conf.addResource(new Path("D:\\web\\hadoop2\\conf\\mapred-site.xml"));
        conf.addResource(new Path("D:\\web\\hadoop2\\conf\\yarn-site.xml"));
//        conf.set("fs.default.name","hdfs://192.168.8.120");
        //必须加上这一句，不然hadoop2会出错
        //mr-jobhistory-daemon.sh start historyserver
//        conf.set("mapred.jar","D:\\web\\hadoop2\\out\\artifacts\\hadoop2_jar\\hadoop2.jar");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job=new Job(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(StringTokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(StringTokenizerReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }


}
