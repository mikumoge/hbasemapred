package com.mycat.hdemo.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestComparator {
    public static void main(String[] args) throws IOException {
        // 获取连接
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181，mycat03:2181");
        Connection cnn = ConnectionFactory.createConnection(config);

        //获取table对象
        Table mk = cnn.getTable(TableName.valueOf("test_mk2"));
    }


     // 多个filter可使用list封装或者可变参数形式传入
    public static void columnFamilyFilter(Table mk) throws IOException {
        Scan scan=new Scan();
        Filter filter=new FamilyFilter(CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("rk002")));
        // 使用多个过滤器的时候
        // 参数1：过滤器之间的关系   Operator.MUST_PASS_ALL Operator.MUST_PASS_ONE
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL,filter,filter);
        //scan.setFilter(filterList);

        // 单一过滤
        scan.setFilter(filter);
        // scan.setFilter(filter);  如果设置两次，后面的设置会将前面的设置覆盖
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
    }

    public static void columnValueFilter(Table mk) throws IOException {
        Scan scan=new Scan();
        Filter filter=new ValueFilter(CompareOperator.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("23")));
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
    }

    // 只根据列名进行比较，不比较列的值
    public static void columnNameFilter(Table mk) throws IOException {
        Scan scan=new Scan();
        Filter filter=new QualifierFilter(CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("rk002")));
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
    }

    public static void scanRowKeyFilter(Table mk) throws IOException {
        // Scan filter
        Scan scan=new Scan();
        Filter filter=new RowFilter(CompareOperator.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("rk002")));
        scan.setFilter(filter);
        ResultScanner mkScanner = mk.getScanner(scan);
        printTraverse(mkScanner);
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
