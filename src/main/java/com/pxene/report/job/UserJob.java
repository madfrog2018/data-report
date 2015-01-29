package com.pxene.report.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.pxene.report.ReportMRHbase;
import com.pxene.report.ReportMRHbase.COUNTERS;
import com.pxene.report.map.UserMapper;

public class UserJob {
	static Logger log = Logger.getLogger(UserJob.class);
	
	public static int userCountJob(Configuration conf,String src_table_name) throws Exception{
		
		
		return 0;
	}
	
	
	
	
	public static int countJob (Configuration conf,String src_table_name) throws Exception{
		
		Job job = Job.getInstance(conf, "tanx_usefull table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(UserMapper.CountMap.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		Scan scan = new Scan();		
//		scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);		  		 		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("mdid|cg"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.CountMap.class,ImmutableBytesWritable.class, KeyValue.class, job);
		
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current countJob result is : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		
		return jobResult;
	}
	
	public static int staticJob(Configuration conf,String src_table_name) throws Exception{
		
		Job job = Job.getInstance(conf, "tanx_usefull table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(UserMapper.CountMap.class);
//		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		Scan scan = new Scan();
		
//		scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);		  		 
		    
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("mdid|cg"));
//		DependentColumnFilter df = new De
		
		scan.setFilter(fi);
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.CountMap.class,ImmutableBytesWritable.class, KeyValue.class, job);
//		TableMapReduceUtil.initTableReducerJob(dist_table_name, Reduce.class,job);
		
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~current countJob result is : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
		
		return jobResult;
	}
	
}
