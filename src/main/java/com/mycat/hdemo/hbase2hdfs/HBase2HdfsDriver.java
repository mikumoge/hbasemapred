package com.mycat.hdemo.hbase2hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HBase2HdfsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://mkmg/");
        conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
        Job job = Job.getInstance(conf);

        job.setJarByClass(HBase2HdfsDriver.class);

        job.setReducerClass(HBase2HdfsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Scan scan=new Scan();
        TableMapReduceUtil.initTableMapperJob("mktest:mk3",scan,HBase2HdfsMapper.class, Text.class, IntWritable.class,job,false);
        FileSystem fs = FileSystem.get(conf);
        Path out=new Path("/mktest/mk4out");
        if(fs.exists(out)){
            fs.delete(out,true);
        }
        FileOutputFormat.setOutputPath(job,out);

        job.waitForCompletion(true);
    }
}
