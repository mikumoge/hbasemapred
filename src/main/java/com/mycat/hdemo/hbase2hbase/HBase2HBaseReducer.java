package com.mycat.hdemo.hbase2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBase2HBaseReducer extends TableReducer<Text, Text , NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Put p=new Put(Bytes.toBytes(key.toString()));
        for (Text value : values) {
            String[] sps = value.toString().split(",");
            p.addColumn(Bytes.toBytes(sps[0]),Bytes.toBytes(sps[1]),Bytes.toBytes(sps[2]));
        }
        context.write(NullWritable.get(),p);
    }
}
