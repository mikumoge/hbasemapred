package com.mycat.hdemo.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Objects;

public class TestComparator2 {
    public static void main(String[] args) throws IOException {
        // 获取连接
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
        Connection cnn = ConnectionFactory.createConnection(config);

        //获取table对象
        Table mk = cnn.getTable(TableName.valueOf("mktest:mk1"));

        rowPrefixFilter(mk);
    }
    // 专用过滤器   PrefixFilter 前缀过滤器
    //              PageFilter   分页过滤器

    public static void pageFilter(Table mk) throws IOException {
        Scan scan=new Scan();
        //主要是针对行键的前缀
        Filter filter=new PageFilter(3); //默认显示第一页，每页3条数据
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
    }

    // 分页查询过滤器
    public static void pageFilter2(Table mk) throws IOException {
        Scan scan=new Scan();
        //主要是针对行键的前缀
        long pageSize=3;
        int currentPage=1;
        scan.withStartRow(Bytes.toBytes(Objects.requireNonNull(getRowKey(currentPage, pageSize, mk))));
        Filter filter=new PageFilter(pageSize); //默认显示第一页，每页3条数据
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
    }

    // 获得每页的第一条的行键
    private static String getRowKey(int currentPage,long pageSize,Table table) throws IOException {
        if(currentPage<=0)currentPage=1;
        Scan scan=new Scan();
        ResultScanner scanner = table.getScanner(scan);
        long index=0L;
        int page=0;// hasPaged---begin from 0,over 3 than add 1  翻了多少页
        for (Result result : scanner) {
            if(index%pageSize==0&&index!=0){
                page++;
            }
            if(page==(currentPage-1)&&index%pageSize==0){
                return Bytes.toString(result.getRow());
            }
            index++;
        }
        return null;
    }

    public static void rowPrefixFilter(Table mk) throws IOException {
        Scan scan=new Scan();

        //主要是针对行键的前缀
        Filter filter=new PrefixFilter(Bytes.toBytes("rk"));
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        for (Result result : mkScanner) {
            System.out.println(Bytes.toString(result.getRow()));
        }
        //printTraverse(mkScanner);
    }

    public static void printTraverse(ResultScanner scanner){
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell c : cells) {
                String name = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                String value = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                System.out.println(name+":"+value);
            }
        }
    }
}
