package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import utils.ConfigurationUtils;

import java.io.*;
import java.net.URI;
import java.net.URL;

/**
 * Created by root on 12/13/15.
 */
public class Hdfs {

    private static String PATH = "hdfs://localhost/tong/core-site.xml";
    //the code should be execute only once at per jvm , it should be put in a static block
    static{
        //declear the hdfs
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    @Test
    public void testGetHDFSFile(){

        InputStream in=null;
        try {
            in = new URL(PATH).openStream();
            IOUtils.copyBytes(in,System.out,1024,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

            IOUtils.closeStream(in);
        }
    }


    @Test
    public void testFSGet(){

        Configuration configuration= ConfigurationUtils.getInstance();
        FileSystem hdfs=null;
        FSDataInputStream inputStream=null;
        try {
            hdfs=FileSystem.newInstance(configuration);
            inputStream = hdfs.open(new Path(URI.create(PATH)));
            IOUtils.copyBytes(inputStream,System.out,2048,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inputStream);
        }

    }


    @Test
    public void testFSGetRandomAccess(){

        Configuration configuration= ConfigurationUtils.getInstance();
        FileSystem hdfs=null;
        FSDataInputStream inputStream=null;
        try {
            hdfs=FileSystem.newInstance(configuration);
            //the inputstream is the instance of FsDataInputStream , which can get the file content random access
            inputStream = hdfs.open(new Path(URI.create(PATH)));
            inputStream.seek(1000);
            IOUtils.copyBytes(inputStream,System.out,2048,false);

            System.out.println();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inputStream);
        }

    }


    @Test
    public void testFSGetRandomRead(){

        Configuration configuration= ConfigurationUtils.getInstance();
        FileSystem hdfs=null;
        FSDataInputStream inputStream=null;
        try {
            hdfs=FileSystem.newInstance(configuration);
            //the inputstream is the instance of FsDataInputStream , which can get the file content random access
            inputStream = hdfs.open(new Path(URI.create(PATH)));
            byte[] buff=new byte[1024];
            while (inputStream.read(buff)!=-1){
                System.out.print(new String(buff));
            }

            System.out.println();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inputStream);
        }

    }

    @Test
    public void testFSAdd(){

        File file=new File("/root/IdeaProjects/hadoop/conf/hdfs-site.xml");
        Configuration configuration= ConfigurationUtils.getInstance();
        FileSystem hdfs=null;
        FSDataOutputStream outputStream=null;
        try {
            hdfs=FileSystem.newInstance(configuration);
            outputStream=hdfs.create(new Path("/tong/hdfs-site.xml"));
            IOUtils.copyBytes(new FileInputStream(file),outputStream,4096);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(outputStream);
        }

    }

    @Test
    public void testFSFileChangedCallback(){
        File file=new File("/root/IdeaProjects/hadoop/conf/hdfs-site.xml");
        Configuration configuration=ConfigurationUtils.getInstance();
        FileSystem fs=null;
        OutputStream outputStream=null;

        try {
            fs=FileSystem.get(configuration);
            outputStream=fs.create(new Path("/tong/hdfs-site.xml"), new Progressable() {
                @Override
                public void progress( ) {
                    System.out.println("This is the callback method!");
                }
            });
            IOUtils.copyBytes(new FileInputStream(file),outputStream,4096);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(outputStream);
        }


    }

    @Test
    public void testFSDel(){

        Configuration configuration= ConfigurationUtils.getInstance();
        FileSystem hdfs=null;
        try {
            hdfs=FileSystem.newInstance(configuration);
            hdfs.delete(new Path("/tong/hdfs-site.xml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }






}
