package StringComp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringComp {

    static class StringCompMapper extends Mapper<Object, Text, NewK, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*Pattern pattern =  Pattern.compile
                    ("([\\d]{11})|([\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2}.[\\d]{9})");
            Matcher matcher = pattern.matcher(value.toString());
            matcher.find();
            String str1 = matcher.group();
            matcher.find();
            String str2 = matcher.group();*/
            String[] list = value.toString().split(" ");
            String str1=list[0];
            String str2=list[3];
            NewK newK = new NewK(str1, str2);
            context.write(newK, value);

        }
    }

    static class StringCompReducer extends Reducer<NewK, Text, Text, NullWritable> {
        @Override
        protected void reduce(StringComp.NewK key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text v : values) {
                context.write(v, NullWritable.get());
            }

        }
    }


    static class NewK implements WritableComparable<NewK> {

        String phone;
        String stayTime;

        public NewK() {
        }

        public NewK(String phone, String stayTime) {
            this.phone = phone;
            this.stayTime = stayTime;
        }

        /**
         * 当k2进行排序时，会调用该方法. * 当第一列不同时，升序；当第一列相同时，第二列降序
         */
        @Override
        public int compareTo(NewK o) {
            final int minus = compTo(this.phone, o.phone);
            if (minus != 0) {
                return minus;
            }
            return -compTo(this.stayTime, o.stayTime);
        } //仿照JDK源码String类的compareTo方法进行实现，

        // 我发现直接使用String类的compareTo方法,并不能得到我想要的结果（第一列升序，第二列降序）。
        public int compTo(String one, String another) {
            int len = one.length();
            char[] v1 = one.toCharArray();
            char[] v2 = another.toCharArray();
            int k = 0;
            while (k < len) {
                char c1 = v1[k];
                char c2 = v2[k];
                if (c1 != c2) {
                    return c1 - c2;
                }
                k++;
            }
            return 0;
        }

        @Override
        public int hashCode() {
            return this.phone.hashCode() + this.stayTime.hashCode();
        }


        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof NewK)) {
                return false;
            }
            NewK oK2 = (NewK) obj;
            return (this.phone == oK2.phone) && (this.stayTime == oK2.stayTime);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.phone);
            out.writeUTF(this.stayTime);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.phone = in.readUTF();
            this.stayTime = in.readUTF();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(configuration, "StringComp");
        job.setJarByClass(StringComp.class);
        job.setMapperClass(StringCompMapper.class);
        job.setMapOutputKeyClass(NewK.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(StringCompReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
