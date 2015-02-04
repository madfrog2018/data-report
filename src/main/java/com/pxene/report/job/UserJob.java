package com.pxene.report.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.pxene.report.ReportMRHbase;
import com.pxene.report.ReportMRHbase.COUNTERS;
import com.pxene.report.map.UserMapper.AppAndCategoryMap;
import com.pxene.report.map.UserMapper.AppUsedCountMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_appcategoryMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_appusedcountMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_deviceIdcountMap;
import com.pxene.report.map.UserMapper.CountMap;
import com.pxene.report.map.UserMapper.DataByTimeMap;
import com.pxene.report.map.UserMapper.DeviceIdCountMap;
import com.pxene.report.map.UserMapper.DeviceIdCount_monthMap;
import com.pxene.report.reduce.UserReducer.AppAndCategoryReduce;
import com.pxene.report.reduce.UserReducer.DeviceIdCountReduce;

public class UserJob {
	static Logger log = Logger.getLogger(UserJob.class);	
	
	/**
	 * 获取 rowkey = appId-appcategory-package 存到中间表里
	 * @param src_table_name ：原表
	 * @param dist_table_name ：目标表 dsp_tanx_app_category
	 * @return
	 * @throws Exception
	 */
	public static int AppAndCategoryJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{
		
		Job job = Job.getInstance(conf, "dsp_tanx_app_category table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(AppAndCategoryMap.class);
		job.setReducerClass(AppAndCategoryReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|cg|mpn"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AppAndCategoryMap.class,Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, AppAndCategoryReduce.class,job);
				 
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ job complete  status is "+ jobResult);
		
		return jobResult;
		
	}
	
	/**
	 * 取出hbase中dsp_tanx_app_category表数据，导入到mysql里
	 */
	public static int ConvertToMysql_appcategoryJob (Configuration conf,String src_table_name) throws Exception{
		Job job = Job.getInstance(conf, "Convert data To Mysql Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ConvertToMysql_appcategoryMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ConvertToMysql_appcategoryMap.class, Text.class, IntWritable.class, job);
		
		int mysqlJob = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current ConvertToMysqlJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		return mysqlJob;
	}
	
	
	
	/**
	 * 每天app被访问的次数
	 * 目标表：dsp_tanx_appused_count
	 */
	public static int AppUsedCountJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{
		Job job = Job.getInstance(conf, "dsp_tanx_appused_count table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(AppUsedCountMap.class);
		job.setReducerClass(AppAndCategoryReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AppUsedCountMap.class,Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, AppAndCategoryReduce.class,job);
				 
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ job complete  status is "+ jobResult);
		
		return jobResult;
		
	}
		
	/**
	 * 取出hbase中dsp_tanx_appused_count表数据，导入到mysql里
	 */
	public static int ConvertToMysql_appusedcountJob (Configuration conf,String src_table_name) throws Exception{
		Job job = Job.getInstance(conf, "Convert data To Mysql Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ConvertToMysql_appusedcountMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ConvertToMysql_appusedcountMap.class, Text.class, IntWritable.class, job);
		
		int mysqlJob = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current ConvertToMysqlJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		return mysqlJob;
	}
	
	
	/**
	 * 每周 /月 访问app的人数
	 * 目标表：dsp_tanx_deviceId_count
	 */
	public static int DeviceIdCountJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{
		//周
//		Job week_job = Job.getInstance(conf, "dsp_tanx_deviceId_week_count table Job");
//		week_job.setJarByClass(ReportMRHbase.class);
//		week_job.setNumReduceTasks(3);
//		week_job.setMapperClass(DeviceIdCountMap.class);
//		week_job.setReducerClass(DeviceIdCountReduce.class);
//	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|mdid"));				
		scan.setFilter(fi);
//		
//		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, DeviceIdCountMap.class,Text.class, Text.class, week_job);
//		TableMapReduceUtil.initTableReducerJob(dist_table_name, DeviceIdCountReduce.class,week_job);
//				 
//		log.info("~~ jobResult configure complete  , waitForCompletion...");
//
//		int week_jobResult = week_job.waitForCompletion(true) ? 0 : 1;
//		log.info("~~ jobResult complete  status is "+ week_jobResult);
		
		//月
		Job month_job = Job.getInstance(conf, "dsp_tanx_deviceId_month_count table Job");
		month_job.setJarByClass(ReportMRHbase.class);
		month_job.setNumReduceTasks(3);
		month_job.setMapperClass(DeviceIdCount_monthMap.class);
		month_job.setReducerClass(DeviceIdCountReduce.class);	
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, DeviceIdCount_monthMap.class,Text.class, Text.class, month_job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, DeviceIdCountReduce.class,month_job);
				 
		log.info("~~ month_jobResult configure complete  , waitForCompletion...");
		
		int month_jobResult = month_job.waitForCompletion(true) ? 0 : 1;		
		log.info("~~ month_jobResult complete  status is "+ month_jobResult);
		
		return month_jobResult;
	}
	
	/**
	 * 取出hbase中dsp_tanx_deviceId_count表数据，导入到mysql里
	 */
	public static int ConvertToMysql_deviceIdcountJob (Configuration conf,String src_table_name) throws Exception{
		Job job = Job.getInstance(conf, "Convert data To Mysql Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ConvertToMysql_deviceIdcountMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ConvertToMysql_deviceIdcountMap.class, Text.class, IntWritable.class, job);
		
		int mysqlJob = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current ConvertToMysqlJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		return mysqlJob;
	}
	
	
	
	/**
	 * 计数job
	 * @param src_table_name ：计数该表数据条数
	 */
	public static int countJob (Configuration conf,String src_table_name) throws Exception{
		
		Job job = Job.getInstance(conf, "count table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(CountMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, CountMap.class,ImmutableBytesWritable.class, KeyValue.class, job);
		
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current countJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		
		return jobResult;
	}
	
	
}
