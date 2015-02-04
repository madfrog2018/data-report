package com.pxene.report.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class UserReducer {
	static Logger log = Logger.getLogger(UserReducer.class);
	static String family = "br";	
	
	
	/**
	 * reduce:
	 * rowkey = appId;appcategory;package 
	 * rowkey = time;appId
	 *
	 */
	public static class AppAndCategoryReduce extends TableReducer<Text, IntWritable, Text> {	
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, Mutation>.Context context)
				throws IOException, InterruptedException {
			
			//相同rowkey合并总是
			int sum = 0;
		    for (IntWritable val : value) {
		      sum += val.get();
		    }
		    result.set(sum);
			
		    //保存到 另一张表里
			Put putrow = new Put(key.getBytes());
			putrow.add(Bytes.toBytes(family), Bytes.toBytes("count"),  Bytes.toBytes(result.toString()));
			
			context.write(key, putrow);
		}
		
	}
	
	
	/**
	 * reduce:
	 * rowkey = time;pid
	 * count:去重复的人数
	 *
	 */	
	public static class DeviceIdCountReduce extends TableReducer<Text, Text, Text> {	
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Mutation>.Context context)
				throws IOException, InterruptedException {
									
			List<byte []> list = new ArrayList<byte []> ();			
		    for (Text val : value) {
	    		 list.add(Bytes.toBytes(val.toString()));
		    }
		    result.set(list.size());
			
		    //保存到 另一张表里
			Put putrow = new Put(key.getBytes());
			putrow.add(Bytes.toBytes(family), Bytes.toBytes("count"),  Bytes.toBytes(result.toString()));
			
			context.write(key, putrow);
		}
	}

	
	
}
