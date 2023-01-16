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

import com.frameworkset.util.SimpleStringUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.nosql.hbase.HBaseHelper;
import org.frameworkset.nosql.hbase.HBaseHelperFactory;
import org.frameworkset.nosql.hbase.TableFactory;
import org.frameworkset.tran.plugin.hbase.HBasePluginConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/30 12:12
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseHelperTest {
	@Test
	public void createTable(){

		Map<String,String> properties = new HashMap<String, String>();
		properties.put("hbase.zookeeper.quorum","192.168.137.133");
		properties.put("hbase.zookeeper.property.clientPort","2183");
		properties.put("zookeeper.znode.parent","/hbase");
		properties.put("hbase.ipc.client.tcpnodelay","true");
		properties.put("hbase.rpc.timeout","10000");
		properties.put("hbase.client.operation.timeout","10000");
		properties.put("hbase.ipc.client.socket.timeout.read","20000");
		properties.put("hbase.ipc.client.socket.timeout.write","30000");
		//异步写入hbase
		/**
		 *     public static final String TABLE_MULTIPLEXER_FLUSH_PERIOD_MS = "hbase.tablemultiplexer.flush.period.ms";
		 *     public static final String TABLE_MULTIPLEXER_INIT_THREADS = "hbase.tablemultiplexer.init.threads";
		 *     public static final String TABLE_MULTIPLEXER_MAX_RETRIES_IN_QUEUE = "hbase.client.max.retries.in.queue";
		 */
		properties.put("hbase.client.async.enable","true");
		properties.put("hbase.client.async.in.queuesize","10000");
		HBasePluginConfig hBasePluginConfig = new HBasePluginConfig();
		hBasePluginConfig.setHbaseClientProperties(properties);
		hBasePluginConfig.setHbaseClientThreadCount(100);
		hBasePluginConfig.setHbaseClientThreadQueue(100);
		hBasePluginConfig.setHbaseClientKeepAliveTime(0L);
		hBasePluginConfig.setHbaseClientBlockedWaitTimeout(1000L);
		hBasePluginConfig.setHbaseClientWarnMultsRejects(1000);
		hBasePluginConfig.setHbaseClientPreStartAllCoreThreads(true);
		hBasePluginConfig.setHbaseClientThreadDaemon(false);



		HBaseHelperFactory.buildHBaseClient(hBasePluginConfig);
		HBaseHelper hBaseHelper = HBaseHelperFactory.getHBaseHelper("default");
		String tableName = "demo";
		hBaseHelper.createTable(tableName,new String[]{"info"});
		tableName = "esdemo";
		hBaseHelper.createTable(tableName,new String[]{"info"});

		tableName = "customdemo";
		hBaseHelper.createTable(tableName,new String[]{"info"});

	}
	@Test
	public void testPutDatas(){
		Map<String,String> properties = new HashMap<String, String>();
		properties.put("hbase.zookeeper.quorum","192.168.137.133");
		properties.put("hbase.zookeeper.property.clientPort","2183");
		properties.put("zookeeper.znode.parent","/hbase");
		properties.put("hbase.ipc.client.tcpnodelay","true");
		properties.put("hbase.rpc.timeout","10000");
		properties.put("hbase.client.operation.timeout","10000");
		properties.put("hbase.ipc.client.socket.timeout.read","20000");
		properties.put("hbase.ipc.client.socket.timeout.write","30000");
		//异步写入hbase
		/**
		 *     public static final String TABLE_MULTIPLEXER_FLUSH_PERIOD_MS = "hbase.tablemultiplexer.flush.period.ms";
		 *     public static final String TABLE_MULTIPLEXER_INIT_THREADS = "hbase.tablemultiplexer.init.threads";
		 *     public static final String TABLE_MULTIPLEXER_MAX_RETRIES_IN_QUEUE = "hbase.client.max.retries.in.queue";
		 */
		properties.put("hbase.client.async.enable","true");
		properties.put("hbase.client.async.in.queuesize","10000");
		HBasePluginConfig hBasePluginConfig = new HBasePluginConfig();
		hBasePluginConfig.setHbaseClientProperties(properties);
		hBasePluginConfig.setHbaseClientThreadCount(100);
		hBasePluginConfig.setHbaseClientThreadQueue(100);
		hBasePluginConfig.setHbaseClientKeepAliveTime(0L);
		hBasePluginConfig.setHbaseClientBlockedWaitTimeout(1000L);
		hBasePluginConfig.setHbaseClientWarnMultsRejects(1000);
		hBasePluginConfig.setHbaseClientPreStartAllCoreThreads(true);
		hBasePluginConfig.setHbaseClientThreadDaemon(false);



		HBaseHelperFactory.buildHBaseClient(hBasePluginConfig);
		HBaseHelper hBaseHelper = HBaseHelperFactory.getHBaseHelper("default");
//		int threadCount, int threadQueue, long keepAliveTime, long blockedWaitTimeout, int warnMultsRejects, boolean preStartAllCoreThreads, Boolean daemon;
		byte[] CF = Bytes.toBytes("Info");
		byte[] C_I = Bytes.toBytes("i");
		byte[] C_j = Bytes.toBytes("j");
		byte[] C_m = Bytes.toBytes("m");
		byte[] C_n = Bytes.toBytes("n");
		final List<Put> datas = new ArrayList<>();
		 for(int i= 0; i < 100; i ++){
		 	 long timestamp = System.currentTimeMillis() ;
			 final byte[] rowKey = Bytes.toBytes(i);
			 final Put put = new Put(rowKey, timestamp);
			 put.addColumn(CF, C_I,timestamp, Bytes.toBytes( "wap_"+i));
			 put.addColumn(CF, C_j,timestamp, Bytes.toBytes( "jdk 1.8_"+i));
			 put.addColumn(CF, C_m,timestamp, Bytes.toBytes( "asdfasfd_"+i));
			 put.addColumn(CF, C_n,timestamp, Bytes.toBytes( i));
			 datas.add(put);
		 }
		hBaseHelper.put("AgentInfo",datas);
	}
	@Test
	public void testHBaseHelper(){
		Map<String,String> properties = new HashMap<String, String>();

		properties.put("hbase.zookeeper.quorum","192.168.137.133");
		properties.put("hbase.zookeeper.property.clientPort","2183");
		properties.put("zookeeper.znode.parent","/hbase");
		properties.put("hbase.ipc.client.tcpnodelay","true");
		properties.put("hbase.rpc.timeout","10000");
		properties.put("hbase.client.operation.timeout","10000");
		properties.put("hbase.ipc.client.socket.timeout.read","20000");
		properties.put("hbase.ipc.client.socket.timeout.write","30000");
		HBasePluginConfig hBasePluginConfig = new HBasePluginConfig();
		hBasePluginConfig.setHbaseClientProperties(properties);
		hBasePluginConfig.setHbaseClientThreadCount(100);
		hBasePluginConfig.setHbaseClientThreadQueue(100);
		hBasePluginConfig.setHbaseClientKeepAliveTime(0L);
		hBasePluginConfig.setHbaseClientBlockedWaitTimeout(1000L);
		hBasePluginConfig.setHbaseClientWarnMultsRejects(1000);
		hBasePluginConfig.setHbaseClientPreStartAllCoreThreads(true);
		hBasePluginConfig.setHbaseClientThreadDaemon(false);



		HBaseHelperFactory.buildHBaseClient(hBasePluginConfig);
		HBaseHelper hBaseHelper = HBaseHelperFactory.getHBaseHelper("default");
		TableFactory tableFactory = hBaseHelper.getTableFactory();
		Table table = tableFactory.getTable(TableName.valueOf("AgentInfo"));
		Scan scan = new Scan();
		scan.setFilter(null);
		try {
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
//				System.out.println("获得到rowkey:" + new String(result.getRow()));
				List<Cell> cells= result.listCells();

//				for (Cell cell : cells) {
//
//					String row = Bytes.toString(result.getRow());
//
//					String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
//
//					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//
//					String value = Bytes.toString(CellUtil.cloneValue(cell));
//
//					System.out.println("[row:"+row+"],[family:"+family1+"],[qualifier:"+qualifier+"]"+ ",[value:"+value+"],[time:"+cell.getTimestamp()+"]");
//
//				}
				byte[] rowKey = result.getRow();
				String agentId = BytesUtils.safeTrim(BytesUtils.toString(rowKey, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
				long reverseStartTime = BytesUtils.bytesToLong(rowKey, HBaseTables.AGENT_NAME_MAX_LEN);
				long startTime = TimeUtils.recoveryTimeMillis(reverseStartTime);

				byte[] serializedAgentInfo = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_IDENTIFIER);
				byte[] serializedServerMetaData = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_SERVER_META_DATA);
				byte[] serializedJvmInfo = result.getValue(HBaseTables.AGENTINFO_CF_INFO, HBaseTables.AGENTINFO_CF_INFO_JVM);

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
				System.out.println(SimpleStringUtil.object2json(agentInfo));
			}
		}
		catch (Throwable e){
			e.printStackTrace();
		}
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
