package com.pxene.report.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseHelper {

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


}
