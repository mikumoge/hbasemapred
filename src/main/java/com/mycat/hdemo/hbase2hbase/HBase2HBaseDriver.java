package com.mycat.hdemo.hbase2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HBase2HBaseDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://mkmg/");
        conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
        Job job = Job.getInstance(conf);

        job.setJarByClass(HBase2HBaseDriver.class);


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        Scan scan=new Scan();
        TableMapReduceUtil.initTableMapperJob("mktest:mk1",scan,HBase2HBaseMapper.class, Text.class,Text.class,job,false);

        TableMapReduceUtil.initTableReducerJob("mktest:mk3",HBase2HBaseReducer.class,job,null,null,null,null,false);

        job.waitForCompletion(true);
    }
}
