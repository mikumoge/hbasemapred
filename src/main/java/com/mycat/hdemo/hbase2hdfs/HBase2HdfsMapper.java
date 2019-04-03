package com.mycat.hdemo.hbase2hdfs;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBase2HdfsMapper extends TableMapper<Text, IntWritable> {// 部门作为key，年龄作为聚合列

    private Text mk=new Text();
    private IntWritable mv=new IntWritable();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Cell[] cells = value.rawCells();
        for (Cell c : cells) {
            String name = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
            String val = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            if(name.equals("dept")){
                mk.set(val);
            }
            if(name.equals("age")){
                mv.set(Integer.parseInt(val));
            }
        }
        context.write(mk,mv);
    }
}
