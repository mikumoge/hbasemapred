package com.mycat.hdemo.hbase2hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBase2HBaseMapper extends TableMapper<Text, Text> {
    private Text mk=new Text();
    private Text mv=new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Cell[] cells = value.rawCells();
        mk.set(Bytes.toString(key.get()));
        for (Cell c : cells) {
            // 列族
            String columnFamily=Bytes.toString(c.getFamilyArray(),c.getFamilyOffset(),c.getFamilyLength());
            // 列名
            String name=Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
            //列值
            String val = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            mv.set(columnFamily+","+name+","+val);
            context.write(mk,mv);
        }
    }
}
