package mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

/**
 * Created by Yue on 2015/12/10.
 */
public class TestConfiguration {


    public static void main(String[] args) {
        TestConfiguration testConfiguration=new TestConfiguration();
        testConfiguration.testGet();
    }
    public void testGet(){
        Configuration conf=new Configuration();
        conf.addResource("D:\\web\\hadoop2\\static\\1.xml");
        System.out.println(conf.get("color"));

    }


}
