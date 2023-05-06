package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.hbase.client.Result;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.EsIdGenerator;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.hbase.input.HBaseInputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: 将保存在hbase中pinpoint AgentInfo信息根据记录时间戳增量同步到elasticsearch中
 * hbase表中列名，由"列族:列名"组成
 * </p>
 * <p>hbase shaded client的版本号与hbase的版本相关，请根据hbase的版本调整hbase shaded client的版本号</p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2ESScrollTimestampDemo {
	private static Logger logger = LoggerFactory.getLogger(HBase2ESScrollTimestampDemo.class);
	public static void main(String[] args){
		HBase2ESScrollTimestampDemo esDemo = new HBase2ESScrollTimestampDemo();
		esDemo.scheduleScrollRefactorImportData();
		System.out.println("complete.");
	}



	public void scheduleScrollRefactorImportData(){
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(1000) //设置批量写入目标Elasticsearch记录数
				.setFetchSize(10000); //设置批量从源Hbase中拉取的记录数,HBase-0.98 默认值为为 100，HBase-1.2 默认值为 2147483647，即 Integer.MAX_VALUE。Scan.next() 的一次 RPC 请求 fetch 的记录条数。配置建议：这个参数与下面的setMaxResultSize配合使用，在网络状况良好的情况下，自定义设置不宜太小， 可以直接采用默认值，不配置。

//		importBuilder.setHbaseBatch(100) //配置获取的列数，假如表有两个列簇 cf，info，每个列簇5个列。这样每行可能有10列了，setBatch() 可以控制每次获取的最大列数，进一步从列级别控制流量。配置建议：当列数很多，数据量大时考虑配置此参数，例如100列每次只获取50列。一般情况可以默认值（-1 不受限），如果设置了scan filter也不需要设置
//				.setMaxResultSize(10000l);//客户端缓存的最大字节数，HBase-0.98 无该项配置，HBase-1.2 默认值为 210241024，即 2M。Scan.next() 的一次 RPC 请求 fetch 的数据量大小，目前 HBase-1.2 在 Caching 为默认值(Integer Max)的时候，实际使用这个参数控制 RPC 次数和流量。配置建议：如果网络状况较好（万兆网卡），scan 的数据量非常大，可以将这个值配置高一点。如果配置过高：则可能 loadCache 速度比较慢，导致 scan timeout 异常
		// 参考文档：https://blog.csdn.net/kangkangwanwan/article/details/89332536


		/**
		 * hbase参数配置
		 */
		HBaseInputConfig hBaseInputConfig = new HBaseInputConfig();
//		hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","192.168.137.133")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
//				.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2183")

		hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","10.13.6.12")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
				.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2185")
				.addHbaseClientProperty("zookeeper.znode.parent","/hbase")
				.addHbaseClientProperty("hbase.ipc.client.tcpnodelay","true")
				.addHbaseClientProperty("hbase.rpc.timeout","10000")
				.addHbaseClientProperty("hbase.client.operation.timeout","10000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.read","20000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.write","30000")

				.setHbaseClientThreadCount(100)  //hbase客户端连接线程池参数设置
				.setHbaseClientThreadQueue(100)
				.setHbaseClientKeepAliveTime(10000l)
				.setHbaseClientBlockedWaitTimeout(10000l)
				.setHbaseClientWarnMultsRejects(1000)
				.setHbaseClientPreStartAllCoreThreads(true)
				.setHbaseClientThreadDaemon(true)

				.setHbaseTable("AgentInfo") //指定需要同步数据的hbase表名称
				;
		//FilterList和filter二选一，只需要设置一种
//		/**
//		 * 设置hbase检索filter
//		 */
//		SingleColumnValueFilter scvf= new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,"wap".getBytes());
//
//		scvf.setFilterIfMissing(true); //默认为false， 没有此列的数据也会返回 ，为true则只返回name=lisi的数据
//
//		hBaseInputConfig.setFilter(scvf);

		/**
		 * 设置hbase组合条件FilterList
		 * FilterList 代表一个过滤器链，它可以包含一组即将应用于目标数据集的过滤器，过滤器间具有“与” FilterList.Operator.MUST_PASS_ALL 和“或” FilterList.Operator.MUST_PASS_ONE 关系
		 */

//		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE); //数据只要满足一组过滤器中的一个就可以
//
//		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,"wap".getBytes());
//
//		list.addFilter(filter1);
//
//		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,Bytes.toBytes("my other value"));
//
//		list.addFilter(filter2);
//		hBaseInputConfig.setFilterList(list);

//		//设置同步起始行和终止行key条件
//		hBaseInputConfig.setStartRow(startRow);
//		hBaseInputConfig.setEndRow(endRow);
		//设置记录起始时间搓（>=）和截止时间搓(<),如果是基于时间范围的增量同步，则不需要指定下面两个参数
//		hBaseInputConfig.setStartTimestamp(startTimestam);
//		hBaseInputConfig.setEndTimestamp(endTimestamp);


		/**
		 * es相关配置
		 * 可以通过addElasticsearchProperty方法添加Elasticsearch客户端配置，
		 * 也可以直接读取application.properties文件中设置的es配置
		 */
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();

//		elasticsearchOutputConfig.addElasticsearchProperty("elasticsearch.rest.hostNames","192.168.137.1:9200");//设置es服务器地址，更多配置参数文档：https://esdoc.bbossgroups.com/#/mongodb-elasticsearch?id=_5242-elasticsearch%e5%8f%82%e6%95%b0%e9%85%8d%e7%bd%ae
		elasticsearchOutputConfig.setTargetElasticsearch("targetElasticsearch");//设置目标Elasticsearch集群数据源名称，和源elasticsearch集群一样都在application.properties文件中配置

		elasticsearchOutputConfig.setIndex("hbase2esdemo"); //全局设置要目标elasticsearch索引名称
//				.setIndexType("hbase2esdemo"); //全局设值目标elasticsearch索引类型名称，如果是Elasticsearch 7以后的版本不需要配置

		// 设置Elasticsearch索引文档_id
		/**
		 * 如果指定rowkey为文档_id,那么需要指定前缀meta:，如果是其他数据字段就不需要
		 * 例如：
		 * meta:rowkey 行key byte[]
		 * meta:timestamp  记录时间戳
		 */
//		elasticsearchOutputConfig.setEsIdField("meta:rowkey");
		// 设置自定义id生成机制
		//如果指定EsIdGenerator，则根据下面的方法生成文档id，
		// 否则根据setEsIdField方法设置的字段值作为文档id，
		// 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id
		elasticsearchOutputConfig.setEsIdGenerator(new EsIdGenerator(){

			@Override
			public Object genId(Context context) throws Exception {
				Object id = context.getMetaValue("rowkey");
				String agentId = BytesUtils.safeTrim(BytesUtils.toString((byte[]) id, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
				return agentId;
			}
		});

		elasticsearchOutputConfig.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false
		elasticsearchOutputConfig.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false

		importBuilder.setOutputConfig(elasticsearchOutputConfig);


		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//hbase表中列名，由"列族:列名"组成
//		//设置任务执行拦截器结束，可以添加多个
//		//增量配置开始
////		importBuilder.setLastValueColumn("Info:id");//指定数字增量查询字段变量名称
		importBuilder.setFromFirst(false);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
//		importBuilder.setLastValueStorePath("hbase2esdemo_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		//指定增量字段类型为日期类型，如果没有指定增量字段名称,则按照hbase记录时间戳进行timerange增量检索
		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);
		// ImportIncreamentConfig.NUMBER_TYPE 数字类型
//		// ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//设置增量查询的起始值时间起始时间
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {

			Date date = format.parse("2010-11-01");
			importBuilder.setLastValue(date);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		//增量配置结束

//		//设置任务执行拦截器，可以添加多个
//		importBuilder.addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException");
//			}
//		}).addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall 1");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall 1");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException 1");
//			}
//		});



		//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
		importBuilder.addFieldValue("author","作者");

		/**
		 * 设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
				//获取原始的hbase记录Result对象
				Result result = (Result) context.getRecord();

				// 直接获取行key，对应byte[]类型，自行提取和分析保存在其中的数据
				byte[] rowKey = (byte[])context.getMetaValue("rowkey");
				String agentId = BytesUtils.safeTrim(BytesUtils.toString(rowKey, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
				context.addFieldValue("agentId",agentId);
				long reverseStartTime = BytesUtils.bytesToLong(rowKey, HBaseTables.AGENT_NAME_MAX_LEN);
				long startTime = TimeUtils.recoveryTimeMillis(reverseStartTime);
				context.addFieldValue("startTime",new Date(startTime));
				// 通过context.getValue方法获取hbase 列的原始值byte[],方法参数对应hbase表中列名，由"列族:列名"组成
				byte[] serializedAgentInfo = (byte[]) context.getValue("Info:i");
				byte[] serializedServerMetaData = (byte[]) context.getValue("Info:m");
				byte[] serializedJvmInfo = (byte[]) context.getValue("Info:j");
				// 通过context提供的一系列getXXXValue方法，从hbase列族中获取相应类型的数据：int,string,long,double,float,date
//				String data = context.getStringValue("Info:i");
				final AgentInfoBo.Builder agentInfoBoBuilder = createBuilderFromValue(serializedAgentInfo);
				agentInfoBoBuilder.setAgentId(agentId);
				agentInfoBoBuilder.setStartTime(startTime);

				if (serializedServerMetaData != null) {
					agentInfoBoBuilder.setServerMetaData(new ServerMetaDataBo.Builder(serializedServerMetaData).build());
				}
				if (serializedJvmInfo != null) {
					agentInfoBoBuilder.setJvmInfo(new JvmInfoBo(serializedJvmInfo));
				}
				AgentInfo agentInfo = new AgentInfo(agentInfoBoBuilder.build());
				context.addFieldValue("agentInfo",agentInfo);
				context.addFieldValue("author","duoduo");
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");


//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				context.addIgnoreFieldMapping("subtitle");
//				/**
//				 * 获取ip对应的运营商和区域信息
//				 */
//				IpInfo ipInfo = context.getIpInfo("Info:agentIp");
//				if(ipInfo != null)
//					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
//				else{
//					context.addFieldValue("ipinfo", "");
//				}
//				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("logOpertime",dateFormat);
//				context.addFieldValue("logOpertime",optime);
//				context.addFieldValue("collecttime",new Date());

				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
			}
		});
		//映射和转换配置结束

		/**
		 * 作业创建一个内置的线程池，实现多线程并行数据导入elasticsearch功能
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false

		/**
		 * 设置任务执行情况回调接口
		 */
		importBuilder.setExportResultHandler(new ExportResultHandler<String,String>() {
			@Override
			public void success(TaskCommand<String,String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
			}

			@Override
			public void error(TaskCommand<String,String> taskCommand, String result) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
			}

			@Override
			public void exception(TaskCommand<String,String> taskCommand, Throwable exception) {
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				logger.info(taskMetrics.toString());
			}


		});
		/**
		 * 执行es数据导入数据库表操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作
	}


	private AgentInfoBo.Builder createBuilderFromValue(byte[] serializedAgentInfo) {
		final Buffer buffer = new FixedBuffer(serializedAgentInfo);
		final AgentInfoBo.Builder builder = new AgentInfoBo.Builder();
		builder.setHostName(buffer.readPrefixedString());
		builder.setIp(buffer.readPrefixedString());
		builder.setPorts(buffer.readPrefixedString());
		builder.setApplicationName(buffer.readPrefixedString());
		builder.setServiceTypeCode(buffer.readShort());
		builder.setPid(buffer.readInt());
		builder.setAgentVersion(buffer.readPrefixedString());
		builder.setStartTime(buffer.readLong());
		builder.setEndTimeStamp(buffer.readLong());
		builder.setEndStatus(buffer.readInt());
		// FIXME - 2015.09 v1.5.0 added vmVersion (check for compatibility)
		if (buffer.hasRemaining()) {
			builder.setVmVersion(buffer.readPrefixedString());
		}
		return builder;
	}
}
