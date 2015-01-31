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
import com.pxene.report.map.UserMapper.CountMap;
import com.pxene.report.map.UserMapper.ExportDataMap;
import com.pxene.report.reduce.UserReducer.ExportDataReduce;

public class UserJob {
	static Logger log = Logger.getLogger(UserJob.class);	
	
	/**
	 * 获取  rowkey = time+cg+mdid  存到中间表里
	 * @param src_table_name ：原表
	 * @param dist_table_name ：目标表
	 * @return
	 * @throws Exception
	 */
	public static int ExportDataJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{
		
		Job job = Job.getInstance(conf, "tanx_usefull table Job");
		job.setJarByClass(ReportMRHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(ExportDataMap.class);
		job.setReducerClass(ExportDataReduce.class);
	
		Scan scan = new Scan();			
//		scan.setTimeRange(NumberUtils.toLong("1417609382670l"), NumberUtils.toLong("1422409225939l"));
		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("mdid|cg"));				
		scan.setFilter(fi);
		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, ExportDataMap.class,Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(dist_table_name, ExportDataReduce.class,job);
				 
		log.info("~~ Job configure complete  , waitForCompletion...");

		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		log.info("~~ job complete  status is "+ jobResult);
		
		return jobResult;
		
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
