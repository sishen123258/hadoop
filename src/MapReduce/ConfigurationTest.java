package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Created by root on 12/14/15.
 */
public class ConfigurationTest {

    @Test
    public void testConfiguration(){

        Configuration configuration=new Configuration();
        configuration.addResource("/root/IdeaProjects/hadoop/static/1.xml");
        System.out.println(configuration.get("color"));
    }

}
