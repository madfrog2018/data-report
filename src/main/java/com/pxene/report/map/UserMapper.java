package com.pxene.report.map;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.pxene.report.ReportMRHbase.COUNTERS;
import com.pxene.report.util.DBUtil;
import com.pxene.report.util.DateUtil;

public class UserMapper {

	static Logger log = Logger.getLogger(UserMapper.class);
	
	private final static Character cgseparator = 0x01;
	private final static String rowkeyseparator = ";";
	
	static DBUtil db = new DBUtil();
	static DateUtil dateUtil  = new DateUtil();
	
	/** 
	 *rowkey = appId;appcategory;package (pid|cg|mpn) 
	 */
	public static class AppAndCategoryMap extends TableMapper<Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
							
//			log.info("~~ current keyIN is "+Bytes.toString(key.get()));				
			List<KeyValue> list = value.list();	
			
			String pidValue = "";
			String mpnValue = "";
			String [] cgValue = new String [5];
			for (KeyValue kv : list) {
				
				String qu = Bytes.toString(kv.getQualifier());				
				if(qu.equals("cg")){
					String cgV = Bytes.toString(kv.getValue());
					if(cgV.contains(String.valueOf(cgseparator))){
						cgValue = qu.split(String.valueOf(cgseparator));
					}else{
						cgValue[0] = cgV;
					}
				}
				else if(qu.equals("pid")){
					pidValue = Bytes.toString(kv.getValue());	
				}
				else if(qu.equals("mpn")){
					mpnValue = Bytes.toString(kv.getValue());	
				}
			}
			for (int i = 0; i < cgValue.length; i++) {
				if(cgValue[i] != null && cgValue[i].trim().length() > 0){
					
					Text resultKey = new Text();
					resultKey.set(pidValue+rowkeyseparator+cgValue[i]+rowkeyseparator+mpnValue);								
					context.write(resultKey,one);
				}
			}			
		}
	}
	
	/**
	 * 数据dsp_tanx_app_category插入到mysql dsp_t_app_category表里
	 */
	public static class ConvertToMysql_appcategoryMap extends TableMapper<Text, IntWritable>{
				
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String rowkey = Bytes.toString(key.get()).trim();
			String [] data = rowkey.split(rowkeyseparator, -1);
			
			db.insertToAppcategory(data[0].trim(),data[1].trim(), data[2].trim());
			
			context.getCounter(COUNTERS.ROWS).increment(1);
			
		}	
	}
	
	
	/**
	 * 每天 每个appid 出现的次数
	 *time;pid
	 */
	public static class AppUsedCountMap extends TableMapper<Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String rowKey =Bytes.toString(key.get());
			List<KeyValue> list = value.list();	
			String pidValue = "";
			for (KeyValue kv : list) {
				String qu = Bytes.toString(kv.getQualifier());	
				if(qu.equals("pid")){
					pidValue = Bytes.toString(kv.getValue());	
				}
			}
			String currentDay = "";
			if(rowKey.length()>32){
				String time = rowKey.substring(32, rowKey.length()).trim();				
				try {
					if(time.matches("[0-9]{1,}")){	
						SimpleDateFormat sf =new SimpleDateFormat("yyyy-MM-dd");	
						Date d =new Date(Long.parseLong(time));
						currentDay = Long.toString(sf.parse(sf.format(d)).getTime());	
						
						Text resultKey = new Text();
						resultKey.set(currentDay+rowkeyseparator+pidValue);								
						context.write(resultKey,one);
						
					}
				} catch (ParseException e) {
						e.printStackTrace();
				}
			}					
		}
	}
	
	/**
	 * 数据dsp_tanx_appused_count插入到mysql dsp_t_app_used_count表里
	 */
	public static class ConvertToMysql_appusedcountMap extends TableMapper<Text, IntWritable>{
				
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			List<KeyValue> list = value.list();	
			String count = Bytes.toString(list.get(0).getValue());
			
			String rowkey = Bytes.toString(key.get()).trim();
			String [] data = rowkey.split(rowkeyseparator, -1);
			
			db.insertToAppusedCount(Long.parseLong(data[0].trim()),data[1].trim(), Integer.valueOf(count));									
		}	
	}
	
	
	/**
	 * 每周/月  访问每个app的人
	 *key:time;pid;mdid
	 *value:1
	 *return：去重复
	 */
	public static class DeviceIdCountMap extends TableMapper<Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String rowKey = Bytes.toString(key.get());						
			String pidValue = "";
			String mdidValue = "";
			
			for (KeyValue kv : value.list()) {
				String qu = Bytes.toString(kv.getQualifier());	
				if(qu.equals("pid")){
					pidValue = Bytes.toString(kv.getValue());	
				}else if(qu.equals("mdid")){
					mdidValue = Bytes.toString(kv.getValue());	
				}
			}
			
			if(rowKey.length()>32){
				String time = rowKey.substring(32, rowKey.length());
				try {
					if(time.matches("[0-9]{1,}")){
						//周
						Text resultKey = new Text();
						resultKey.set(dateUtil.convertWeekByTime(time)+rowkeyseparator+pidValue+rowkeyseparator+Bytes.toBytes(mdidValue));								
						context.write(resultKey,one);	
						
						//月
						Text resultKey2 = new Text();
						resultKey2.set(dateUtil.convertMonthByTime(time)+rowkeyseparator+pidValue+rowkeyseparator+Bytes.toBytes(mdidValue));								
						context.write(resultKey2,one);
						
					}
				} catch (ParseException e) {
						e.printStackTrace();
				}
			}			
		}
	}	
	/**
	 * 每周/月  访问每个app的人数
	 *key:time;pid
	 *value:1
	 *return：sum
	 */
	public static class DeviceIdCount_weekMap extends TableMapper<Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {		
			
			String [] keys = (Bytes.toString(key.get())).split(rowkeyseparator, -1);
		
			Text resultKey = new Text();
			resultKey.set(keys[0]+rowkeyseparator+keys[1]);								
			context.write(resultKey,one);			
		}
	}
			
	/**
	 * 数据dsp_tanx_deviceId_count插入到mysql dsp_t_app_deviceId_count表里
	 */
	public static class ConvertToMysql_deviceIdcountMap extends TableMapper<Text, IntWritable>{
				
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			List<KeyValue> list = value.list();	
			String count = Bytes.toString(list.get(0).getValue());
			String rowkey = Bytes.toString(key.get()).trim();
			String [] data = rowkey.split(rowkeyseparator, -1);
			
			db.insertToDeviceIdCount(Long.parseLong(data[0].trim()),data[1].trim(), Integer.valueOf(count));					
		}	
	}		
	
	
	/** 
	 *rowkey = time(一天的0点);appid;mdid
	 *同app，同用户，同一天去重复次数
	 */
	public static class AppUsed_DistinctDay_JobMap extends TableMapper<Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String rowKey = Bytes.toString(key.get());	
			String pidValue = "";
			String mdidValue = "";
			
			for (KeyValue kv : value.list()) {
				String qu = Bytes.toString(kv.getQualifier());	
				
				if(qu.equals("pid")){
					pidValue = Bytes.toString(kv.getValue());	
				}else if(qu.equals("mdid")){
					mdidValue = Bytes.toString(kv.getValue());	
				}
			}
			if(rowKey.length()>32){
				String time = rowKey.substring(32, rowKey.length());						
				try {
					if(time.matches("[0-9]{1,}")){		
						Text resultKey = new Text();
						resultKey.set(dateUtil.convertDayByTime(time)+rowkeyseparator+pidValue+rowkeyseparator+mdidValue);
						
						context.write(resultKey,one);
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}
	}
	/** 
	 *rowkey = time(周一的0点);appid
	 *合并出一周 使用同一个app的个数 ，即是天数
	 */
	public static class AppUsed_CountDays_JobMap extends TableMapper<Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String rowKey = Bytes.toString(key.get());	
			String [] keys = rowKey.split(rowkeyseparator,-1);										
			try {
				if(keys[0].matches("[0-9]{1,}")){	
					//周
					Text resultKey = new Text();
					resultKey.set(dateUtil.convertWeekByTime(keys[0])+rowkeyseparator+keys[1]);				
					context.write(resultKey,one);
					
					//月
					Text resultKey2 = new Text();
					resultKey2.set(dateUtil.convertMonthByTime(keys[0])+rowkeyseparator+keys[1]);				
					context.write(resultKey2,one);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}
	
	/**
	 * 数据dsp_tanx_deviceId_count插入到mysql dsp_t_app_deviceId_count表里
	 */
	public static class ConvertToMysql_usedDayscountMap extends TableMapper<Text, IntWritable>{
				
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			List<KeyValue> list = value.list();	
			String count = Bytes.toString(list.get(0).getValue());
			String rowkey = Bytes.toString(key.get()).trim();
			String [] data = rowkey.split(rowkeyseparator, -1);
			
			db.insertToUsedDaysCount(Long.parseLong(data[0].trim()),data[1].trim(), Integer.valueOf(count));					
		}	
	}	
	
	
	/**
	 * 计数map
	 */
	public static class CountMap extends TableMapper<ImmutableBytesWritable, KeyValue> {
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValue>.Context context)
				throws IOException, InterruptedException {
		
				context.getCounter(COUNTERS.ROWS).increment(1);
			}
	}
	

	
	
//	/**
//	 * 通过列族获取该列信息，暂不用
//	 */
//	public static class getKVMap extends TableMapper<ImmutableBytesWritable, KeyValue> {
//		
//		@Override
//		protected void map(
//				ImmutableBytesWritable key,
//				Result value,
//				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValue>.Context context)
//				throws IOException, InterruptedException {
//			
//			NavigableMap<byte[], byte[]>  map=	 value.getFamilyMap(Bytes.toBytes("br"));
//			for (byte[] qu  : map.keySet()) {
//				log.info("~~ map current value is :"+ map.get(qu));
//				context.write(
//						key,
//						new KeyValue(value.getRow(),qu, map.get(qu)));
//			}
//		}
//	}


}
