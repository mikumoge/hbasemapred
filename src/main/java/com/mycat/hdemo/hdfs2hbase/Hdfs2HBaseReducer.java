package com.mycat.hdemo.hdfs2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Hdfs2HBaseReducer extends TableReducer<Text,NullWritable,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String[] wr = key.toString().split(",");
        Put p=new Put(Bytes.toBytes(wr[0]));
        if(wr.length==5){
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("name"),Bytes.toBytes(wr[1]));
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("sex"),Bytes.toBytes(wr[2]));
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("age"),Bytes.toBytes(wr[3]));
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("dept"),Bytes.toBytes(wr[4]));
            context.write(NullWritable.get(),p);
        }
    }
}
