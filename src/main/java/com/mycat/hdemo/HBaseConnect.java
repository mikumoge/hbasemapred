package  com.mycat.hdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HBaseConnect {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
        Connection cnn = ConnectionFactory.createConnection(conf);
        Table table = cnn.getTable(TableName.valueOf("mktest:mk1"));

        Scan scan=new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
        }
    }

    @Test
    public void addData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat01:2181,mycat03:2181");
        Connection cnn = ConnectionFactory.createConnection(conf);
        Table table = cnn.getTable(TableName.valueOf("mktest:mk1"));
        Put p=new Put(Bytes.toBytes("rk005"));
        p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("name"),Bytes.toBytes("nike"));
        p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("age"),Bytes.toBytes("26"));
        p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("sex"), Bytes.toBytes("female"));

        table.put(p);
        table.close();
    }
}
