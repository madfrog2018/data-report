package com.pxene.report.job;

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
import com.pxene.report.map.UserMapper.AppUsed_CountDays_Map;
import com.pxene.report.map.UserMapper.ConvertToMysql_appcategoryMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_appusedcountMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_deviceIdcountMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_deviceIdcountbydayMap;
import com.pxene.report.map.UserMapper.ConvertToMysql_useddayscountMap;
import com.pxene.report.map.UserMapper.CountMap;
import com.pxene.report.map.UserMapper.DeviceIdByTime_CountMap;
import com.pxene.report.map.UserMapper.DeviceIdCountMap;
import com.pxene.report.map.UserMapper.DistinctByDay_Map;
import com.pxene.report.reduce.UserReducer.DistinctReduce;
import com.pxene.report.reduce.UserReducer.SumReduce;

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
		job.setReducerClass(SumReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|cg|mpn"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AppAndCategoryMap.class,Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, SumReduce.class,job);
				 
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
		job.setReducerClass(SumReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AppUsedCountMap.class,Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, SumReduce.class,job);
				 
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
	 * 中间去重表 ： dsp_tanx_deviceId_distinct
	 * 目标表：dsp_tanx_deviceId_count
	 */
	public static int DeviceIdCountJob(Configuration conf,String src_table_name,String middle_table_name,String dist_table_name) throws Exception{
		//周/月   distinct
		Job distinct_job = Job.getInstance(conf, "dsp_tanx_deviceId_distinct_count table Job");
		distinct_job.setJarByClass(ReportMRHbase.class);
		distinct_job.setNumReduceTasks(3);
		distinct_job.setMapperClass(DeviceIdCountMap.class);
		distinct_job.setReducerClass(DistinctReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|mdid"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, DeviceIdCountMap.class,Text.class, IntWritable.class, distinct_job);
		TableMapReduceUtil.initTableReducerJob(middle_table_name, DistinctReduce.class,distinct_job);
				 
		log.info("~~ jobResult configure complete  , waitForCompletion...");

		int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);
		
		
		//周/月 count 
		Job count_job = Job.getInstance(conf, "dsp_tanx_deviceId_count table Job");
		count_job.setJarByClass(ReportMRHbase.class);
		count_job.setNumReduceTasks(3);
		count_job.setMapperClass(DeviceIdByTime_CountMap.class);
		count_job.setReducerClass(SumReduce.class);
		
		Scan week_scan = new Scan();			
		QualifierFilter fit =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		week_scan.setFilter(fit);
			
		TableMapReduceUtil.initTableMapperJob(middle_table_name, week_scan, DeviceIdByTime_CountMap.class,Text.class, IntWritable.class, count_job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, SumReduce.class,count_job);
				 
		log.info("~~ jobResult configure complete  , waitForCompletion...");

		int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ count_jobResult complete  status is "+ count_jobResult);
		
		return count_jobResult;
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
	 * 每周/月 访问app的总天数
	 * 中间去重表 ： dsp_tanx_usedDay_distinct
	 * 目标表：dsp_tanx_usedDay_count
	 */
	public static int AppUsedDaysCountJob(Configuration conf,String src_table_name,String middle_table_name,String dist_table_name) throws Exception{
		//周/月  distinct(去重复 同一个人 同一天 使用同一个app 记成一条记录)
		Job distinct_job = Job.getInstance(conf, "dsp_tanx_distinct_days table Job");
		distinct_job.setJarByClass(ReportMRHbase.class);
		distinct_job.setNumReduceTasks(3);
		distinct_job.setMapperClass(DistinctByDay_Map.class);
		distinct_job.setReducerClass(DistinctReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|mdid"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, DistinctByDay_Map.class,Text.class, IntWritable.class, distinct_job);
		TableMapReduceUtil.initTableReducerJob(middle_table_name, DistinctReduce.class,distinct_job);
				 
		log.info("~~ distinct_job configure complete  , waitForCompletion...");

		int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);
		
		
//		周/月  count(合并出一周 使用同一个app的个数 ，即是天数)
		Job count_job = Job.getInstance(conf, "dsp_tanx_days_count table Job");
		count_job.setJarByClass(ReportMRHbase.class);
		count_job.setNumReduceTasks(3);
		count_job.setMapperClass(AppUsed_CountDays_Map.class);
		count_job.setReducerClass(SumReduce.class);
	
		Scan count_scan = new Scan();			
		QualifierFilter fit =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		count_scan.setFilter(fit);
		
		TableMapReduceUtil.initTableMapperJob(middle_table_name, count_scan, AppUsed_CountDays_Map.class,Text.class, IntWritable.class, count_job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, SumReduce.class,count_job);
				 
		log.info("~~ count_job configure complete  , waitForCompletion...");

		int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ count_job complete  status is "+ count_jobResult);
		
		return count_jobResult;
	}
	
	/**
	 * 取出hbase中dsp_tanx_usedDay_count表数据，导入到mysql里
	 */
	public static int ConvertToMysql_useddayscountJob (Configuration conf,String src_table_name) throws Exception{
		Job job = Job.getInstance(conf, "Convert data To Mysql Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ConvertToMysql_useddayscountMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ConvertToMysql_useddayscountMap.class, Text.class, IntWritable.class, job);
		
		int mysqlJob = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current ConvertToMysqlJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		return mysqlJob;
	}
	
	
	/**
	 * 日使用人数 
	 * 中间去重表 ： dsp_tanx_deviceIdByDay_distinct
	 * 目标表：dsp_tanx_deviceIdByDay_count
	 */
	public static int DeviceIdCountByDayJob(Configuration conf,String src_table_name,String middle_table_name,String dist_table_name) throws Exception{
		//日人数  distinct(去重复 同一个人 同一天 使用同一个app 记成一条记录)
		Job distinct_job = Job.getInstance(conf, "dsp_tanx_distinct_days table Job");
		distinct_job.setJarByClass(ReportMRHbase.class);
		distinct_job.setNumReduceTasks(3);
		distinct_job.setMapperClass(DistinctByDay_Map.class);
		distinct_job.setReducerClass(DistinctReduce.class);
	
		Scan scan = new Scan();			
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("pid|mdid"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, DistinctByDay_Map.class,Text.class, IntWritable.class, distinct_job);
		TableMapReduceUtil.initTableReducerJob(middle_table_name, DistinctReduce.class,distinct_job);
				 
		log.info("~~ distinct_job configure complete  , waitForCompletion...");

		int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);
		
		
//		日  count(合并出一天 使用同一个app的个数 ，即是人数)
		Job count_job = Job.getInstance(conf, "dsp_tanx_days_count table Job");
		count_job.setJarByClass(ReportMRHbase.class);
		count_job.setNumReduceTasks(3);
		count_job.setMapperClass(DeviceIdByTime_CountMap.class);
		count_job.setReducerClass(SumReduce.class);
	
		Scan count_scan = new Scan();			
		QualifierFilter fit =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		count_scan.setFilter(fit);
		
		TableMapReduceUtil.initTableMapperJob(middle_table_name, count_scan, DeviceIdByTime_CountMap.class,Text.class, IntWritable.class, count_job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, SumReduce.class,count_job);
				 
		log.info("~~ count_job configure complete  , waitForCompletion...");

		int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ count_job complete  status is "+ count_jobResult);
		
		return distinct_jobResult;
	}
	
	/**
	 * 取出hbase中dsp_tanx_deviceIdByDay_count表数据，导入到mysql里
	 */
	public static int ConvertToMysql_deviceIdCountByDayJob (Configuration conf,String src_table_name) throws Exception{
		Job job = Job.getInstance(conf, "Convert data To Mysql Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ConvertToMysql_deviceIdcountbydayMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("count"));				
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ConvertToMysql_deviceIdcountbydayMap.class, Text.class, IntWritable.class, job);
		
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
		
		log.info("~~ countJob configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current countJob counters are : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		
		return jobResult;
	}
	
	
}
