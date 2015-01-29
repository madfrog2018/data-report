package com.pxene.report.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class UserReducer {
	static Logger log = Logger.getLogger(UserReducer.class);
	
	
	public static class UserCountReduce extends Reducer<Text, IntWritable, ImmutableBytesWritable, KeyValue>{
		@Override
		protected void reduce(
				Text key,
				Iterable<IntWritable> value,
				Reducer<Text, IntWritable, ImmutableBytesWritable, KeyValue>.Context context)
				throws IOException, InterruptedException {

			
			
			
		}
		
	}
	
	
	public static class Reduce extends TableReducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable> {
		
		@Override
		protected void reduce(
				ImmutableBytesWritable key,
				Iterable<KeyValue> values,
				Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
		
				log.info("~~ current k=========" + key.get());
			
				Put putrow = new Put(key.get());
				for (KeyValue t : values) {
					log.info("~~ current  value is" + t.getValueArray().toString());
					
					if(Bytes.toString(t.getQualifierArray()).equals("mdid")){
						putrow.add(t.getFamilyArray(), Bytes.toBytes("mdid"), t.getValueArray());
						log.info("~~ current mdid value is" + t.getValueArray().toString());
					}
					if(Bytes.toString(t.getQualifierArray()).equals("cg")){
						putrow.add(t.getFamilyArray(), Bytes.toBytes("cg"), t.getValueArray());
						log.info("~~ current mdid value is" + t.getValueArray().toString());
					}	
				}
				try {
					context.write(key, putrow);
			
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	
	
	
}
