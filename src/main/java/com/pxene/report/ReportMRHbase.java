package com.pxene.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

import com.pxene.report.job.UserJob;
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
	
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene02,pxene03,pxene04,pxene05");

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
		
		String dist_table_name = otherArgs[otherArgs.length - 1];
		String src_table_name = otherArgs[otherArgs.length - 2]; // "dsp_tanx_bidrequest_log";		
		
		HBaseHelper Hhelper = HBaseHelper.getHelper(conf);
		Hhelper.creatTable(dist_table_name,family);
		
//		int jobResult = UserJob.AppAndCategoryJob (conf,src_table_name,dist_table_name);		
//		log.info("~~current ExportDataJob status is : "+ jobResult);
				
		//将jobResult结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_appcategoryJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);
		
		
		//每天app被访问的次数
		int appUsedCountJob = UserJob.AppUsedCountJob(conf, src_table_name,dist_table_name);		
		log.info("~~current appCountJob status is : "+ appUsedCountJob);
		
		//将appCountJob结果导入到mysql中
//		int ConToMysqlJob = UserJob.ConvertToMysql_appusedcountJob(conf, dist_table_name);
//		log.info("~~current Convert data ToMysql Job status is : "+ ConToMysqlJob);
	
		
		//每天访问app的人数
//		int mdidCountJob = UserJob.DeviceIdCountJob(conf, src_table_name,dist_table_name);		
//		log.info("~~current appCountJob status is : "+ mdidCountJob);
		
		

					
		
		//----计数job
		int countJob = UserJob.countJob(conf, dist_table_name);		
		log.info("~~current countJob status is : "+ countJob);
		
		
		return countJob;
	}




	
	
}
