package com.pxene.report.map;

import java.io.IOException;
import java.util.ArrayList;
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

public class UserMapper {

	static Logger log = Logger.getLogger(UserMapper.class);
	
	private final static String cgeparator = "/";
	
	/**
	 * rowkey = "time+cg+mdid" 
	 * count =（ 同一时间 +同一app ）操作的人数 one
	 */
	public static class ExportDataMap extends TableMapper<Text, IntWritable> {
				
		private final static IntWritable one = new IntWritable(1);
		private Text resultKey = new Text();
		
		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
							
			log.info("~~ current keyIN is "+Bytes.toString(key.get()));	
			
			List<KeyValue> list = value.list();	
			
			String rowKey =Bytes.toString(key.get());
			String mdidValue = "";
			String [] cgValue = new String [5];
			for (KeyValue kv : list) {
				
				String qu = Bytes.toString(kv.getQualifier());				
				if(qu.equals("cg")){
					String cgV = Bytes.toString(kv.getValue());
					if(cgV.contains(cgeparator)){
						cgValue = qu.split(cgeparator);
					}else{
						cgValue[0] = cgV;
					}
				}
				if(qu.equals("mdid")){
					mdidValue = Bytes.toString(kv.getValue());	
				}
			}
			for (int i = 0; i < cgValue.length; i++) {
				if(cgValue[i] != null && cgValue[i].trim().length() > 0){
					resultKey.set(rowKey.substring(32, rowKey.length())+"+"+cgValue[i]+"+"+mdidValue);				
					
					context.write(resultKey,one);
				}
			}			
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
