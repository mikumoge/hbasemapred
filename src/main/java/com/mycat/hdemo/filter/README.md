#### HBase Filter分类
##### 1）比较过滤器  -------------TestComarator
包括：     -RowFilter  行键过滤器
           -QualiferFilter 列名过滤器
           -FamilyFilter 列族过滤器
           -ValueFilter  值过滤器
使用：Filter  filter = new 过滤器(过滤器规则，过滤依据);
###### ① 过滤器规则 枚举 2.x版本为CompareOperator(老本版叫CompareOp)
成员          解释                         对应比较运算符
LESS      	即 less than *                       <
GREATER     即 greater than *                    >
EQUALS		即 equals *                          ==
GREATER_OR_EQUAL	即 greater or equals to *    >=
LESS_OR_EQUAL	即 less or equals to *           <=
NOT_EQUAL	即	not equals to *                  !=
NO_OP  		即 没有比较
例如使用行键过滤器：Filter  filter = new RowFilter(CompareOperator.EQUALS,过滤依据);

###### ② 过滤器依据
BinaryComparator 按字节索引顺序比较指定字节数组，采用 Bytes.compareTo(byte[])   最常用  顺序比较每一个字符
----
BinaryPrefixComparator 跟前面相同，只是比较左端的数据是否相同
NullComparator 判断给定的是否为空
BitComparator 按位比较    比较少用
RegexStringComparator 提供一个正则的比较器，仅支持 EQUAL 和非 EQUAL
SubstringComparator 判断提供的子串是否出现在 value 中。
----
例如使用行键过滤器：Filter  filter = new RowFilter(CompareOperator.EQUALS,new BinaryComparator(Bytes.toBytes("rk001")));

##### 2）专用过滤器  -------------TestComarator2
包括：
PrefixFilter   前缀过滤器   只能针对行键前缀  过滤   使用详见代码
PageFilter 	分页过滤器       使用详见代码