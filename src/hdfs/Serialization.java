package hdfs;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;

import java.io.*;

/**
 * Created by Yue on 2015/12/14.
 */
public class Serialization extends TestCase{

    public void testSerialization() throws IOException {

        IntWritable intWritable=new IntWritable(1000);
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutputStream dataOutputStream=new DataOutputStream(out);
        intWritable.write(dataOutputStream);
        System.out.println(out.toByteArray());
        out.writeTo(new FileOutputStream("D:\\web\\hadoop2\\static\\serization"));
        dataOutputStream.close();


    }

    public void testDeserialization() throws IOException {
        FileInputStream fileInputStream=new FileInputStream("D:\\web\\hadoop2\\static\\serization");
        byte[] buff=new byte[1024];
        fileInputStream.read(buff);
        IntWritable intWritable=new IntWritable();
        ByteArrayInputStream in = new ByteArrayInputStream(buff);
        DataInputStream dataIn = new DataInputStream(in);
        intWritable.readFields(dataIn);
        dataIn.close();
        System.out.println(intWritable.get());
    }




}
