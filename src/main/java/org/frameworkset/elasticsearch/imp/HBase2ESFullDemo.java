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

import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.EsIdGenerator;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.es.ESField;
import org.frameworkset.tran.hbase.HBaseExportBuilder;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;

/**
 * <p>Description: 将保存在hbase中pinpoint AgentInfo信息定时全量同步到elasticsearch中</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2ESFullDemo {
	public static void main(String[] args){
		HBase2ESFullDemo esDemo = new HBase2ESFullDemo();
		esDemo.scheduleScrollRefactorImportData();
		System.out.println("complete.");
	}



	public void scheduleScrollRefactorImportData(){
		HBaseExportBuilder importBuilder = new HBaseExportBuilder();
		importBuilder.setBatchSize(1000) //设置批量从源Elasticsearch中拉取的记录数
				.setFetchSize(5000); //设置批量写入目标Elasticsearch记录数

		/**
		 * hbase参数配置
		 */
		importBuilder.addHbaseClientProperty("hbase.zookeeper.quorum","192.168.137.133")
				.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2183")
				.addHbaseClientProperty("zookeeper.znode.parent","/hbase")
				.addHbaseClientProperty("hbase.ipc.client.tcpnodelay","true")
				.addHbaseClientProperty("hbase.rpc.timeout","10000")
				.addHbaseClientProperty("hbase.client.operation.timeout","10000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.read","20000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.write","30000")
				.setHbaseTable("AgentInfo")
				.setHbaseClientThreadCount(100)
				.setHbaseClientThreadQueue(100)
				.setHbaseClientKeepAliveTime(10000l)
				.setHbaseClientBlockedWaitTimeout(10000l)
				.setHbaseClientWarnMultsRejects(1000)
				.setHbaseClientPreStartAllCoreThreads(true)
				.setHbaseClientThreadDaemon(true)
				//.setIncrementFamilyName("Info") //如果是增量同步，需要指定增量同步的
				;
		/**
		 * es相关配置
		 */
		importBuilder.setIndex("hbase2esfulldemo") //全局设置要目标elasticsearch索引名称
					 .setIndexType("hbase2esfulldemo"); //全局设值目标elasticsearch索引类型名称，如果是Elasticsearch 7以后的版本不需要配置
		importBuilder.setTargetElasticsearch("targetElasticsearch");//设置目标Elasticsearch集群数据源名称，和源elasticsearch集群一样都在application.properties文件中配置



		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall");
			}

			@Override
			public void throwException(TaskContext taskContext, Exception e) {
				System.out.println("throwException");
			}
		}).addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall 1");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Exception e) {
				System.out.println("throwException 1");
			}
		});
//		//设置任务执行拦截器结束，可以添加多个
//		//增量配置开始
////		importBuilder.setNumberLastValueColumn("logId");//指定数字增量查询字段变量名称
//		importBuilder.setDateLastValueColumn("logOpertime");//手动指定日期增量查询字段变量名称
//		importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
//		importBuilder.setLastValueStorePath("hbase2esdemo_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		/**
		 * 如果指定索引文档元数据字段为为文档_id,那么需要指定前缀meta:，如果是其他数据字段就不需要
		 * **文档_id*
		 *private String id;
		 *    **文档对应索引类型信息*
		 *private String type;
		 *    **文档对应索引字段信息*
		 *private Map<String, List<Object>> fields;
		 * **文档对应版本信息*
		 *private long version;
		 *  **文档对应的索引名称*
		 *private String index;
		 *  **文档对应的高亮检索信息*
		 *private Map<String, List<Object>> highlight;
		 *     **文档对应的排序信息*
		 *private Object[] sort;
		 *     **文档对应的评分信息*
		 *private Double score;
		 *     **文档对应的父id*
		 *private Object parent;
		 *     **文档对应的路由信息*
		 *private String routing;
		 *     **文档对应的是否命中信息*
		 *private boolean found;
		 *     **文档对应的nested检索信息*
		 *private Map<String, Object> nested;
		 *     **文档对应的innerhits信息*
		 *private Map<String, Map<String, InnerSearchHits>> innerHits;
		 *     **文档对应的索引分片号*
		 *private String shard;
		 *     **文档对应的elasticsearch集群节点名称*
		 *private String node;
		 *    **文档对应的打分规则信息*
		 *private Explanation explanation;
		 *
		 *private long seqNo;//"_index": "trace-2017.09.01",
		 *private long primaryTerm;//"_index": "trace-2017.09.01",
		 */
//		importBuilder.setEsIdField("meta:rowkey");
//		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
//		//设置增量查询的起始值lastvalue
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		try {
//
//			Date date = format.parse("2000-01-01");
//			importBuilder.setLastValue(date);
//		}
//		catch (Exception e){
//			e.printStackTrace();
//		}

		//增量配置结束

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
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}

				byte[] rowKey = (byte[])context.getMetaValue("rowkey");
				String agentId = BytesUtils.safeTrim(BytesUtils.toString(rowKey, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
				context.addFieldValue("agentId",agentId);
				long reverseStartTime = BytesUtils.bytesToLong(rowKey, HBaseTables.AGENT_NAME_MAX_LEN);
				long startTime = TimeUtils.recoveryTimeMillis(reverseStartTime);
				context.addFieldValue("startTime",new Date(startTime));
				byte[] serializedAgentInfo = (byte[]) context.getValue("Info:i");
				byte[] serializedServerMetaData = (byte[]) context.getValue("Info:m");
				byte[] serializedJvmInfo = (byte[]) context.getValue("Info:j");

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
//				Map ipInfo = (Map)context.getValue("ipInfo");
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

		importBuilder.setEsIdGenerator(new EsIdGenerator(){

			@Override
			public Object genId(Context context) throws Exception {
				ClientOptions clientOptions = context.getClientOptions();
				ESField esIdField = clientOptions != null?clientOptions.getIdField():null;
				if (esIdField != null) {
					Object id = null;
					if(!esIdField.isMeta())
						id = context.getValue(esIdField.getField());
					else
						id = context.getMetaValue(esIdField.getField());
					String agentId = BytesUtils.safeTrim(BytesUtils.toString((byte[]) id, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
					return agentId;
				}
				return null;
			}
		});
		/**
		 * 一次、作业创建一个内置的线程池，实现多线程并行数据导入elasticsearch功能，作业完毕后关闭线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回
//		importBuilder.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false，不打印响应报文将大大提升性能，只有在调试需要的时候才打开，log日志级别同时要设置为INFO
//		importBuilder.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认true，如果不需要响应报文将大大提升处理速度
		importBuilder.setPrintTaskLog(true);
		importBuilder.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false
		importBuilder.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false

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
