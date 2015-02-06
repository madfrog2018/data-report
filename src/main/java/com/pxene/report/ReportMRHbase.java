package com.pxene.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

import com.pxene.report.job.UserJob;
import com.pxene.report.util.Driver;
import com.pxene.report.util.HBaseHelper;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author shanhongshu
 * 2015-01-26
 */
public class ReportMRHbase extends Configured implements Tool{
	
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene02,pxene03,pxene04,pxene05");//Driver.config();

//	private static Configuration conf = HBaseHelper.getHBConfig("slave2,slave1,master");

	static Logger log = Logger.getLogger(ReportMRHbase.class);
	public static enum COUNTERS {ROWS};
	public static String family = "br";
	
	public static void main(String[] args) throws Exception {

		ToolRunner.run(new ReportMRHbase(), args);		
		
	}
	

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		conf.set("mapred.child.java.opts", "-Xmx512m");
//		conf.set("mapreduce.map.java.opts ","-Xmx3072m");
//		conf.set("mapreduce.reduce.java.opts", "-Xmx6144m");	
//		conf.set("yarn.nodemanager.vmem-pmem-ratio", "7.1");
//		conf.set("yarn.scheduler.minimum-allocation-mb", "4096");
//		conf.set("yarn.scheduler.maximum-allocation-mb", "24G");	
//		conf.set("yarn.nodemanager.resource.memory-mb", "24G");
		
//		String dist_table_name = otherArgs[otherArgs.length - 1];
//		String src_table_name = otherArgs[otherArgs.length - 2]; // "dsp_tanx_bidrequest_log";		
		
//		HBaseHelper Hhelper = HBaseHelper.getHelper(conf);
//		Hhelper.creatTable(dist_table_name,family);
		
//		int jobResult = UserJob.AppAndCategoryJob (conf,src_table_name,dist_table_name);		
//		log.info("~~current ExportDataJob status is : "+ jobResult);
				
		//将jobResult结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_appcategoryJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);
		
		
		//每天app被访问的次数
//		int appUsedCountJob = UserJob.AppUsedCountJob(conf, src_table_name,dist_table_name);		
//		log.info("~~current appCountJob status is : "+ appUsedCountJob);
		
		//将appUsedCountJob结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_appusedcountJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);

//----------------------------------
		
		String dist_table_name = otherArgs[otherArgs.length - 1];
//		String src_table_name = otherArgs[otherArgs.length - 3]; // "dsp_tanx_bidrequest_log";		
//		String middle_table_name = otherArgs[otherArgs.length - 2];
//		
//		HBaseHelper Hhelper = HBaseHelper.getHelper(conf);
//		Hhelper.creatTable(dist_table_name,family);
//		Hhelper.creatTable(middle_table_name,family);

		
//		//每周/月  访问app的人数（去重复）
//		int mdidCountJob = UserJob.DeviceIdCountJob(conf, src_table_name,middle_table_name,dist_table_name);		
//		log.info("~~current mdidCountJob status is : "+ mdidCountJob);		
		
		//将mdidCountJob结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_deviceIdcountJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);				
		
		
		//每周/月  人访问app的总天数sum
//		int appUsed_DaysCount_Job = UserJob.AppUsedDaysCountJob(conf, src_table_name,middle_table_name,dist_table_name);		
//		log.info("~~current appUsed_DaysCount_Job status is : "+ appUsed_DaysCount_Job);		
//		Hhelper.deleteTable(middle_table_name);
		
		//将appUsed_DaysCount_Job结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_useddayscountJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);
		
		
//------------------------------------
		
		//日 访问app人数
//		int mdidCountByDay_Job = UserJob.DeviceIdCountByDayJob(conf, src_table_name,middle_table_name,dist_table_name);		
//		log.info("~~current mdidCountByDayJob status is : "+ mdidCountByDay_Job);	
				
		//将mdidCountByDay_Job结果导入到mysql中
		int ConToMysqlJob = UserJob.ConvertToMysql_deviceIdCountByDayJob(conf, dist_table_name);
		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);
		
		
		//删除中间表
//		Hhelper.deleteTable(middle_table_name);
				
		
		//----计数job
		int countJob = UserJob.countJob(conf, dist_table_name);		
		log.info("~~current countJob status is : "+ countJob);
				
		return countJob;
	}




	
	
}
