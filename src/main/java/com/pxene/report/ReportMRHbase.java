package com.pxene.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import com.pxene.report.job.UserJob;
import com.pxene.report.util.HBaseHelper;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReportMRHbase extends Configured implements Tool{
	
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene03,pxene04");
	
	static Logger log = Logger.getLogger(ReportMRHbase.class);
	public static enum COUNTERS {ROWS};

	
	public static void main(String[] args) throws Exception {

		ToolRunner.run(new ReportMRHbase(), args);		
		
	}
	

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String src_table_name = otherArgs[otherArgs.length - 1]; // "dsp_tanx_bidrequest_log";
		
		int jobResult = UserJob.countJob (conf,src_table_name);
		
		log.info("~~current Job status is : "+jobResult);
		
		return jobResult;
	}
//	
//	public static int staticJob(String src_table_name) throws Exception{
//		
//		Job job = Job.getInstance(conf, "tanx_usefull table Job");
//		job.setJarByClass(ReportMRHbase.class);
//		job.setNumReduceTasks(0);
//		job.setMapperClass(UserMapper.CountMap.class);
////		job.setReducerClass(Reduce.class);
//		job.setOutputFormatClass(NullOutputFormat.class);
//		Scan scan = new Scan();
//		
////		scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);		  		 
//		    
//		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("mdid|cg"));
////		DependentColumnFilter df = new De
//		
//		scan.setFilter(fi);
//		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.CountMap.class,ImmutableBytesWritable.class, KeyValue.class, job);
////		TableMapReduceUtil.initTableReducerJob(dist_table_name, Reduce.class,job);
//		
//		log.info("~~ Job configure complete  , waitForCompletion...");
//
//		int jobResult = job.waitForCompletion(true) ? 0 : 1;
//		log.info("~~current countJob result is : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
//		
//		return jobResult;
//	}
	
//	public static int countJob (String src_table_name) throws Exception{
//		
//		Job job = Job.getInstance(conf, "tanx_usefull table Job");
//		job.setJarByClass(ReportMRHbase.class);
//		job.setNumReduceTasks(0);
//		job.setMapperClass(CountMapper.CountMap.class);
//		job.setOutputFormatClass(NullOutputFormat.class);
//		Scan scan = new Scan();
//		
////		scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);		  		 
//		    
//		QualifierFilter fi =new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("mdid|cg"));
////		DependentColumnFilter df = new De
//		
//		scan.setFilter(fi);
//		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, CountMapper.CountMap.class,ImmutableBytesWritable.class, KeyValue.class, job);
//		
//		log.info("~~ Job configure complete  , waitForCompletion...");
//
//		int jobResult = job.waitForCompletion(true) ? 0 : 1;
//		log.info("~~current countJob result is : " + job.getCounters().findCounter(COUNTERS.ROWS).getValue());
//		
//		return jobResult;
//	}
	
	
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
	
//	public static class CountMap extends
//			TableMapper<ImmutableBytesWritable, KeyValue> {
//
//		@Override
//		protected void map(
//				ImmutableBytesWritable key,
//				Result value,
//				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValue>.Context context)
//				throws IOException, InterruptedException {
//
//			context.getCounter(COUNTERS.ROWS).increment(1);
//		}
//	}

//	public static class Reduce
//			extends
//			TableReducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable> {
//		@Override
//		protected void reduce(
//				ImmutableBytesWritable key,
//				Iterable<KeyValue> values,
//				Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, Mutation>.Context context)
//				throws IOException, InterruptedException {
//
//			log.info("~~ current k=========" + key.get());
//
//			Put putrow = new Put(key.get());
//			for (KeyValue t : values) {
//				log.info("~~ current  value is" + t.getValueArray().toString());
//				
//				if(Bytes.toString(t.getQualifierArray()).equals("mdid")){
//					putrow.add(t.getFamilyArray(), Bytes.toBytes("mdid"), t.getValueArray());
//					log.info("~~ current mdid value is" + t.getValueArray().toString());
//				}
//				if(Bytes.toString(t.getQualifierArray()).equals("cg")){
//					putrow.add(t.getFamilyArray(), Bytes.toBytes("cg"), t.getValueArray());
//					log.info("~~ current mdid value is" + t.getValueArray().toString());
//				}	
//			}
//			try {
//				context.write(key, putrow);
//
//			} catch (IOException e) {
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	/**
	 * 新增表
	 */
	public static void creatTable(String tableName, String family) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				log.info("~~ table already exists!");
				deleteTable(tableName);
			}
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			tableDesc.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDesc);

			log.info("~~ create table " + tableName + " ok.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除表
	 */
	@SuppressWarnings("resource")
	public static void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			log.info("~~ delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	
	
}
