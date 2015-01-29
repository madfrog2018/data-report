package com.pxene.report.map;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.pxene.report.ReportMRHbase.COUNTERS;

public class UserMapper {
	static Logger log = Logger.getLogger(UserMapper.class);
	
	
	public static class UserCountMap extends TableMapper<ImmutableBytesWritable, KeyValue>{
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValue>.Context context)
				throws IOException, InterruptedException {
			
			
			
		}
		
	}
			
	
	
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
	
	public static class getKVMap extends TableMapper<ImmutableBytesWritable, KeyValue> {
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValue>.Context context)
				throws IOException, InterruptedException {
			
			NavigableMap<byte[], byte[]>  map=	 value.getFamilyMap(Bytes.toBytes("br"));
			for (byte[] qu  : map.keySet()) {
				log.info("~~ map current value is :"+ map.get(qu));
				context.write(
						key,
						new KeyValue(value.getRow(),qu, map.get(qu)));
			}
		}
	}
	
}
