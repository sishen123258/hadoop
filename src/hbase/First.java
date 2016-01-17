/*
package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

*/
/**
 * Created by Yue on 2015/12/6.
 *//*

public class First {

    public static void main(String[] args) throws IOException {

        Configuration configuration= HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.8.120");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "192.168.8.120:60000");

        HTableInterface hTable=new HTable(configuration,"user");
        Put put=new Put(Bytes.toBytes("Mark Twain"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("Mark Twain"));
        hTable.put(put);
        hTable.close();

    }


}
*/
