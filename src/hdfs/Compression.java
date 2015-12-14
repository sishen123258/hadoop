package hdfs;

import junit.framework.TestCase;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.*;

/**
 * Created by Yue on 2015/12/14.
 */
public class Compression extends TestCase{

    public void testGZipCodec() throws IOException {
        GzipCodec gzipCodec=new GzipCodec();
        InputStream inputStream=new BufferedInputStream(new FileInputStream("D:\\web\\hadoop2\\conf\\core-site.xml"));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("D:\\web\\hadoop2\\static\\test.gzip"));
        CompressionOutputStream outputStream = gzipCodec.createOutputStream(out);
        byte buff[]=new byte[1000];
        while (inputStream.read(buff)!=-1){
            outputStream.write(buff,0,buff.length);
        }
        outputStream.flush();
        outputStream.close();
        inputStream.close();


    }





}
