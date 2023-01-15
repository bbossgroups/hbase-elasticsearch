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
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.hbase.output.HBaseOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description:
 * db数据到hbase同步案例
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2HbaseDemo {
	private static Logger logger = LoggerFactory.getLogger(DB2HbaseDemo.class);
	public static void main(String args[]){
		DB2HbaseDemo dbdemo = new DB2HbaseDemo();
		boolean dropIndice = true;//CommonLauncher.getBooleanAttribute("dropIndice",false);//同时指定了默认值

		dbdemo.scheduleTimestampImportData(dropIndice);
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(boolean dropIndice){


		ImportBuilder importBuilder = new ImportBuilder();
		// 5.2.4.1 设置hbase参数
		HBaseOutputConfig hBaseOutputConfig = new HBaseOutputConfig();
		hBaseOutputConfig.setName("targethbase");
		hBaseOutputConfig.setFamiliy("info").setHbaseTable("demo") ;//指定需要同步数据的hbase表名称;
		hBaseOutputConfig.setRowKeyField("LOG_ID")
				.addHbaseClientProperty("hbase.zookeeper.quorum","192.168.137.133")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
				.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2183")
				.addHbaseClientProperty("zookeeper.znode.parent","/hbase")
				.addHbaseClientProperty("hbase.ipc.client.tcpnodelay","true")
				.addHbaseClientProperty("hbase.rpc.timeout","10000")
				.addHbaseClientProperty("hbase.client.operation.timeout","10000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.read","20000")
				.addHbaseClientProperty("hbase.ipc.client.socket.timeout.write","30000")
				.addHbaseClientProperty("hbase.client.async.enable","true")
				.addHbaseClientProperty("hbase.client.async.in.queuesize","10000")
				.setHbaseClientThreadCount(100)  //hbase客户端连接线程池参数设置
				.setHbaseClientThreadQueue(100)
				.setHbaseClientKeepAliveTime(10000l)
				.setHbaseClientBlockedWaitTimeout(10000l)
				.setHbaseClientWarnMultsRejects(1000)
				.setHbaseClientPreStartAllCoreThreads(true)
				.setHbaseClientThreadDaemon(true);

		importBuilder.setOutputConfig(hBaseOutputConfig);

/**
 * db input相关配置
 */
		/**
		 * 源db相关配置
		 */
		DBInputConfig dbInputConfig = new DBInputConfig();
		dbInputConfig
				.setSqlFilepath("sql-dbtran.xml")
				.setSqlName("demoexport");
		dbInputConfig.setDbName("source")
				.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
				.setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
				.setDbUser("root")
				.setDbPassword("123456")
				.setValidateSQL("select 1")
				.setColumnLableUpperCase(false)
				.setUsePool(true);//是否使用连接池
		importBuilder.setInputConfig(dbInputConfig);

		importBuilder.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(100)  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
				.setFetchSize(100); //按批从mongodb拉取数据的大小
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束


		//增量配置开始
		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段
		importBuilder.setFromFirst(false);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
		importBuilder.setLastValueStorePath("db2hbase_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型,ImportIncreamentConfig.TIMESTAMP_TYPE为时间类型
		//设置增量查询的起始值lastvalue

		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//增量配置结束

		//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 *
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
//		importBuilder.addIgnoreFieldMapping("testInt");
//
//		/**
//		 * 重新设置es数据结构
//		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
//				String id = context.getStringValue("_id");
//				//根据字段值忽略对应的记录，这条记录将不会被同步到elasticsearch中
//				if(id.equals("5dcaa59e9832797f100c6806"))
//					context.setDrop(true);
				//添加字段extfiled2到记录中，值为2
				context.addFieldValue("extfiled2",2);
				//添加字段extfiled到记录中，值为1
				context.addFieldValue("extfiled",1);



			}
		});
		//映射和转换配置结束

		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回

		//设置任务处理结果回调接口
		importBuilder.setExportResultHandler(new ExportResultHandler<Object,String>() {
			@Override
			public void success(TaskCommand<Object,String> taskCommand, String result) {
				logger.info(taskCommand.getTaskMetrics().toString());//打印任务执行情况
			}

			@Override
			public void error(TaskCommand<Object,String> taskCommand, String result) {
				logger.info(taskCommand.getTaskMetrics().toString());//打印任务执行情况

			}

			@Override
			public void exception(TaskCommand<Object,String> taskCommand, Throwable exception) {
				logger.info(taskCommand.getTaskMetrics().toString(),exception);//打印任务执行情况
			}

			@Override
			public int getMaxRetry() {
				return 0;
			}
		});
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {

			}

			@Override
			public void afterCall(TaskContext taskContext) {
				logger.info(taskContext.getJobTaskMetrics().toString());//打印任务执行情况
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				logger.info(taskContext.getJobTaskMetrics().toString(),e);//打印任务执行情况
			}
		});

		/**
		 * 构建DataStream，执行db数据到hbase的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作
		System.out.println();
	}

}
