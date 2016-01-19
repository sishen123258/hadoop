package domain.Join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 当输入路径有对个时
 */
public class JoinMR {

    public static void main(String[] args) {

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
