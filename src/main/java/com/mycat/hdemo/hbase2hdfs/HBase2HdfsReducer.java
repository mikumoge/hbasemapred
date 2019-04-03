package com.mycat.hdemo.hbase2hdfs;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HBase2HdfsReducer extends Reducer<Text, IntWritable,Text, NullWritable> {// 制表符分割输出
    private Text mk=new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0; // 求每组（dept）总和
        int count=0; //计数
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        double avg=sum*1.0/count;
        mk.set(key.toString()+"\t"+Double.toString(avg));
        context.write(mk,NullWritable.get());
    }
}
