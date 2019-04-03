package com.mycat.hdemo.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hdfs2HBaseDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://mkmg/");
        conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Hdfs2HBaseDriver.class);

        job.setMapperClass(Hdfs2HBaseMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job,new Path("/mktest/student.txt"));
        FileSystem system = FileSystem.get(conf);
        Path pt=new Path("/mktest/stuout");
        if(system.exists(pt)){
            system.delete(pt,true);
        }

        FileOutputFormat.setOutputPath(job,pt);

        TableMapReduceUtil.initTableReducerJob("mktest:mk1",Hdfs2HBaseReducer.class,job,null,null,null,null,false);
        job.waitForCompletion(true);
    }
}
