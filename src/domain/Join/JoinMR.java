package domain.Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 当输入路径有对个时 MultipleInputs
 *
 */
public class JoinMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args=new String[]{ "/tong/sogo/split1","/tong/sogo/split2","/tong/sogo_join"};

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

        Job job=new Job();
        job.setJarByClass(JoinMR.class);
        //TextInputFormat
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, JoinMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, JoinMapper2.class);

        job.setReducerClass(JoinReduce.class);
        //TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class JoinMapper1 extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] contents = str.split("\t");
            String joinKey=contents[0];
            String content=contents[1]+"\t"+contents[2];
            context.write(new Text(joinKey),new Text(content));
        }
    }

    public static class JoinMapper2 extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] contents = str.split("\t");
            String joinKey=contents[0];
            String content=contents[1]+"\t"+contents[2]+"\t"+contents[3]+"\t"+contents[4];
            context.write(new Text(joinKey),new Text(content));
        }
    }

    /**
     * 这里的join是一对一得关联
     * 在Reduce因为shuffle的存在，使得只会出现一对一和一对多的关联关系
     *若是一对多，则使用list保存多的那个对象，然后再去构建例如xml和json这种层级结构的数据
     */
    public static class JoinReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text v:values){
                sb.append(v.toString());
                sb.append("\t");
            }
            context.write(new Text(sb.toString()),NullWritable.get());
        }
    }


}
