##### hbase 到 hdfs ,即使用hbase的自定义输入类即可
---- 需求是按照部门求平均年龄
map端：（TableMapper  行键 value 行结果集，即一行数据记录）
    key:部门
    value:年龄

reduce端：
    key:部门
    value:平均值
Driver端：
1）hdfs和zookeeper配置
    conf.set("fs.defaultFS","hdfs://mkmg/"); //我的是高可用集群，完全分布式的就是NameNode直接与端口的配置了
    conf.set("hbase.zookeeper.quorum","mycat01:2181,mycat02:2181,mycat03:2181");
2）输入输出格式

最后一个参数是不添加HBASE的依赖的jar包----解决jar包冲突
TableMapReduceUtil.initTableMapperJob("mktest:mk1",scan,HBase2HBaseMapper.class, Text.class,Text.class,job,false);