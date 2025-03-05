package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.plugin.hbase.input.HBaseInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: elasticsearch到sftp数据上传案例</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2FileFtpBatchSplitFileDemo {
	public static void main(String[] args){
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(500).setFetchSize(1000);
		String ftpIp = CommonLauncher.getProperty("ftpIP","localhost:7001");//同时指定了默认值
		FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();
		FtpOutConfig ftpOutConfig = new FtpOutConfig();
		ftpOutConfig.setFtpIP(ftpIp);

		ftpOutConfig.setFtpPort(5322);
//		ftpOutConfig.addHostKeyVerifier("2a:da:5a:6a:cf:7d:65:e5:ac:ff:d3:73:7f:2c:55:c9");
		ftpOutConfig.setFtpUser("ecs");
		ftpOutConfig.setFtpPassword("ecs@123");
		ftpOutConfig.setRemoteFileDir("/home/ecs/failLog");
		ftpOutConfig.setKeepAliveTimeout(100000);
		ftpOutConfig.setTransferEmptyFiles(true);
		ftpOutConfig.setFailedFileResendInterval(-1);
		ftpOutConfig.setBackupSuccessFiles(true);

		ftpOutConfig.setSuccessFilesCleanInterval(5000);
		ftpOutConfig.setFileLiveTime(86400);//设置上传成功文件备份保留时间，默认2天
		fileFtpOupputConfig.setFileDir("D:\\workdir");
		fileFtpOupputConfig.setMaxFileRecordSize(20);//每千条记录生成一个文件
		fileFtpOupputConfig.setDisableftp(false);//false 启用sftp/ftp上传功能,true 禁止（只生成数据文件，保留在FileDir对应的目录下面）
		//自定义文件名称
		fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
			@Override
			public String genName( TaskContext taskContext,int fileSeq) {
				//fileSeq为切割文件时的文件递增序号
				String time = (String)taskContext.getTaskData("time");//从任务上下文中获取本次任务执行前设置时间戳
				String _fileSeq = fileSeq+"";
				int t = 6 - _fileSeq.length();
				if(t > 0){
					String tmp = "";
					for(int i = 0; i < t; i ++){
						tmp += "0";
					}
					_fileSeq = tmp+_fileSeq;
				}



				return "hbase" + "_"+time +"_" + _fileSeq+".txt";
			}
		});
		//指定文件中每条记录格式，不指定默认为json格式输出
		fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
				//直接将记录按照json格式输出到文本文件中
				SerialUtil.normalObject2json(record.getDatas(),//获取记录中的字段数据
						builder);
				String data = (String)taskContext.getTaskData("data");//从任务上下文中获取本次任务执行前设置时间戳
//          System.out.println(data);

			}
		});
		importBuilder.setOutputConfig(fileFtpOupputConfig);
		importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据
		/**
		 * hbase参数配置
		 */
		HBaseInputConfig hBaseInputConfig = new HBaseInputConfig();
//		hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","192.168.137.133")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
//				.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2183")
		hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","101.13.6.12")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
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
		importBuilder.setInputConfig(hBaseInputConfig);
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(30000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				String formate = "yyyyMMddHHmmss";
				//HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
				SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
				String time = dateFormat.format(new Date());
				taskContext.addTaskData("time",time);
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				System.out.println("throwException 1");
			}
		});


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
		importBuilder.addFieldValue("author","张无忌");
//		importBuilder.addFieldMapping("operModule","OPER_MODULE");
//		importBuilder.addFieldMapping("logContent","LOG_CONTENT");
//		importBuilder.addFieldMapping("logOperuser","LOG_OPERUSER");


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
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,"test",
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,"test",
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
			}
		});
		//映射和转换配置结束

		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(false);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true);

		/**
		 * 启动es数据导入文件并上传sftp/ftp作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//启动同步作业
	}

	private  static AgentInfoBo.Builder createBuilderFromValue(byte[] serializedAgentInfo) {
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
