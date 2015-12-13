package hdfs;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.ConfigurationUtils;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * The	 FileStatus 	class
 encapsulates	filesystem	metadata	for	files	and	directories,	including	file	length,	block	size,
 replication,	modification	time,	ownership,	and	permission	information
 */
public class FileStatusTest {

    private String PATH = "/tong/core-site.xml";
    private FileSystem fs;

    @Before
    public void setUp(){
        try {
            fs=FileSystem.get(ConfigurationUtils.getInstance());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown(){
        if(fs != null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //if you want to get the test passed , you should throws the exeception
    @Test(expected = FileNotFoundException.class)
    public void test() throws IOException {
        fs.getFileStatus(new Path("/tong/tong"));
    }

    @Test
    public void testGetFileStatus(){

        try {
            FileStatus fileStatus = fs.getFileStatus(new Path(PATH));
            System.out.println(DateFormatUtils.format(fileStatus.getAccessTime(),"yyyy-MM-dd HH:mm:ss"));
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getBlockSize()/1024/1024); //128M
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getReplication());
//            System.out.println(fileStatus.getSymlink());
            System.out.println(fileStatus.isDirectory());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void listFiles(){
        try {
            FileStatus[] fileStatus = fs.listStatus(new Path("/tong"));
            for (FileStatus fstatus : fileStatus){
                System.out.println(fstatus.getPath());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * when process a batch of files,maybe the Pattern is a good way to math these files
     */

    @Test
    public void testFFlush(){
        try {
            Path path = new Path("/tong/flush");
            FSDataOutputStream outputStream = fs.create(path);
            outputStream.write("content".getBytes("utf8"));
            outputStream.flush();
            FileStatus fileStatus=fs.getFileStatus(path);
            System.out.println(fileStatus.getLen());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHFlush(){
        try {
            Path path = new Path("/tong/flush1");
            FSDataOutputStream outputStream = fs.create(path);
            outputStream.write("content".getBytes("utf8"));
            //does not gurrent write the data to disk,write to memory ,so the data maybe lost
            outputStream.hflush();
            //not gurrent write the data to disk
//            outputStream.hsync();
            FileStatus fileStatus=fs.getFileStatus(path);
            System.out.println(fileStatus.getLen());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
