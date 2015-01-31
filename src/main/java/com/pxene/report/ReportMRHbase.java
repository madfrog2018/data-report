package com.pxene.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import com.pxene.report.job.UserJob;
import com.pxene.report.util.HBaseHelper;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author shanhongshu
 * 2015-01-26
 */
public class ReportMRHbase extends Configured implements Tool{
	
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene03,pxene04");
	
	static Logger log = Logger.getLogger(ReportMRHbase.class);
	public static enum COUNTERS {ROWS};
	public static String family = "br";
	
	public static void main(String[] args) throws Exception {

		ToolRunner.run(new ReportMRHbase(), args);		
		
	}
	

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String src_table_name = otherArgs[otherArgs.length - 1]; // "dsp_tanx_bidrequest_log";
		String dist_table_name = otherArgs[otherArgs.length - 2];
		
		HBaseHelper Hhelper = HBaseHelper.getHelper(conf);
		
		Hhelper.creatTable(dist_table_name,family);
		
		int jobResult = UserJob.ExportDataJob (conf,src_table_name,dist_table_name);
		
		log.info("~~current ExportDataJob status is : "+ jobResult);
		
		int countJob = UserJob.countJob(conf, dist_table_name);
		
		log.info("~~current countJob status is : "+ countJob);
		
		return jobResult;
	}




	
	
}
