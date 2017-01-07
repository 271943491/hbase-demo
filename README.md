# hbase-demo

HbaseApi 通过hadoop的mapReduce，将计算结构存入到hbase

y1.txt:
page20|5|1.2678936|1
page84|1|4.2879653|0
page90|5|1.6330373|-1


hbaseMapReduce hbase自带mapreduce操作

blog2表结构:

hbase(main):055:0> scan 'blog2'
ROW                                                          COLUMN+CELL
 rowkey1                                                     column=article:content, timestamp=1471875534306, value=HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.
 rowkey1                                                     column=article:tag, timestamp=1471875534306, value=Hadoop,HBase,NoSQL
 rowkey1                                                     column=article:title, timestamp=1471875534306, value=Head First HBase
 rowkey1                                                     column=author:name, timestamp=1471875534306, value=nicholas
 rowkey1                                                     column=author:nickname, timestamp=1471875534306, value=lee
1 row(s) in 0.0870 seconds
