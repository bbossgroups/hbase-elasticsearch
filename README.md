
Bboss is a good elasticsearch Java rest client. It operates and accesses elasticsearch in a way similar to mybatis.
#
# BBoss Environmental requirements

JDK requirement: JDK 1.7+

Elasticsearch version requirements: 1.x,2.X,5.X,6.X,7.x,+

Spring booter 1.x,2.x,+
# HBase-Elasticsearch 数据同步工具demo
使用本demo所带的应用程序运行容器环境，可以快速编写，打包发布可运行的数据导入工具，包含现成的示例如下：
## jdk timer定时全量同步
org.frameworkset.elasticsearch.imp.HBase2ESFullDemo
## jdk timer定时增量同步
org.frameworkset.elasticsearch.imp.HBase2ESScrollTimestampDemo
## jdk timer定时增量同步（简化demo，hbase1.x,hbase2.x都可以跑）
org.frameworkset.elasticsearch.imp.HBase2ESScrollTimestampDemo223
## jdk timer定时带条件同步
org.frameworkset.elasticsearch.imp.HBase2ESFullDemoWithFilter
## quartz定时全量同步
org.frameworkset.elasticsearch.imp.QuartzHBase2ESImportTask
## 支持的数据库：
HBase 到elasticsearch数据同步
## 支持的Elasticsearch版本：
1.x,2.x,5.x,6.x,7.x,+


[使用参考文档](https://esdoc.bbossgroups.com/#/hbase-elasticsearch)


## elasticsearch技术交流群:166471282 

## elasticsearch微信公众号:bbossgroup   
![GitHub Logo](https://static.oschina.net/uploads/space/2017/0617/094201_QhWs_94045.jpg)


