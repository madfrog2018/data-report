package com.pxene.report.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.pxene.report.ReportMRHbase;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseHelper {
	static Logger log = Logger.getLogger(HBaseHelper.class);
	
	public static byte[] INSERT_TIME = Bytes.toBytes(5);
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	public static final String TABLENAME_USER = "dsp_tanx_bidrequest_log";
	public static final String TABLENAME_TAG = "dmp_tag";
	public static final byte[] FAMILY_URL = Bytes.toBytes("url");
	public static final String TYPE_FIX = "fixed";
	public static final String SPILE="#";
	protected HBaseHelper(Configuration conf) throws IOException {
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}

	public static HBaseHelper getHelper(Configuration conf) throws IOException {
		return new HBaseHelper(conf);
	}

	public static Configuration getDefaultHBConfig() {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				ConfigUtil.getByKey("hbase.zookeeper.quorum"));
		return config;
	}

	public static Configuration getHBConfig(String quorum) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", quorum);
//		config.set("zookeeper.znode.parent", "/hbase_wins");
		return config;
	}

	public HConnection getCon() throws IOException {
		if (conf == null) {
			conf = getDefaultHBConfig();
		}
		return HConnectionManager.createConnection(conf);
	}


	public static void close(HBaseCloseable closeable) throws IOException {
		if (closeable.getScanner() != null)
			closeable.getScanner().close();
		if (closeable.getTableInterface() != null)
			closeable.getTableInterface().close();
		if (closeable.getConnection() != null) {
			closeable.getConnection().close();
		}
	}
	


	/**
	 * 新增表
	 */
	public  void creatTable(String tableName, String family) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				log.info("~~ table already exists!");
				deleteTable(tableName);
			}
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			tableDesc.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDesc);

			log.info("~~ create table " + tableName + " ok.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除表
	 */
	@SuppressWarnings("resource")
	public  void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			log.info("~~ delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

		
	
	/**
	 * 根据时间判断是否超时
	 * 
	 * @param time
	 * @param needclosetable
	 * @param key
	 * @param fa
	 * @param qu
	 * @return
	 * @throws IOException
	 */
	public static Expires expires(long time, HTableInterface needclosetable,
			byte[] key, byte[] fa, byte[] qu) throws IOException {
		Get get = new Get(key);
		Result re = needclosetable.get(get);
		byte[] v = re.getValue(fa, qu);
		if (v != null && Bytes.toLong(v) >= time) {
			return new Expires(true, re);
		}
		needclosetable.close();
		return new Expires(false, re);
	}

	private static class Expires {
		public boolean expires;
		public Result result;

		public Expires(boolean e, Result re) {
			expires = e;
			result = re;
		}
	}
	public static Scan setTimeRang(Scan sca, long start, long end)
			throws IOException {
		sca.setStartRow(Bytes.toBytes(start + SPILE));
		sca.setStopRow(Bytes.toBytes(end + SPILE));
		return sca;
	}

	@SuppressWarnings("deprecation")
	public static String getRealRowKey(KeyValue kv) {  
        int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset()+KeyValue.ROW_OFFSET);  
        String rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset()+KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT, rowlength);  
        return rowKey;  
	}
	
	public  String getStringFromBytes(byte [] bytes)
	{
		String resultString = new String(bytes, 0, bytes.length, Charset.forName("utf-8"));
		return resultString;
	}
	
	
	static final long[] byteTable = createLookupTable();
	static final long HSTART = 0xBB40E64DA205B064L;
	static final long HMULT = 7664345821815920749L;
	
	private static final long[] createLookupTable() {
		long[] byteTable = new long[256];
		long h = 0x544B2FBACAAF1684L;
		for (int i = 0; i < 256; i++) {
			for (int j = 0; j < 31; j++) {
				h = (h >>> 7) ^ h;
				h = (h << 11) ^ h;
				h = (h >>> 10) ^ h;
			}
			byteTable[i] = h;
		}
		return byteTable;
	}
	
	public static long hashCode(CharSequence cs) {
		long h = HSTART;
		final long hmult = HMULT;
		final long[] ht = byteTable;
		final int len = cs.length();
		for (int i = 0; i < len; i++) {
			char ch = cs.charAt(i);
			h = (h * hmult) ^ ht[ch & 0xff];
			h = (h * hmult) ^ ht[(ch >>> 8) & 0xff];
		}
		return h;
	}

}
