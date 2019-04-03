#### 源数据是 student.txt， 先从本地上传到hdfs上，然后导入到hbase的表

HBase到HBase：
map端：（TableMapper ImmutableBytesWritable key:行键  Result value:每一行，即每一条记录）
    由于是由HBase写入到HBase，所以需要传递行键，每一行的话，由于结果集需要传递，所以把其拆成一定格式传递到reduce
    key:行键
    value:封装 列族名，列名，列值

reduce端：（TableReducer Text key:map输出的行键，value：`键值对`的迭代器,上层是以逗号分割的）
    key:行键
    value:  列族名，列名，列值  的迭代器（相同行键）

Driver端：
1）hdfs和zookeeper配置
    conf.set("fs.defaultFS","hdfs://mkmg/"); //我的是高可用集群，完全分布式的就是NameNode直接与端口的配置了
    conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
2）输入输出格式

最后一个参数是不添加HBASE的依赖的jar包----解决jar包冲突
TableMapReduceUtil.initTableMapperJob("mktest:mk1",scan,HBase2HBaseMapper.class, Text.class,Text.class,job,false);
TableMapReduceUtil.initTableReducerJob("mktest:mk3",HBase2HBaseReducer.class,job,null,null,null,null,false);