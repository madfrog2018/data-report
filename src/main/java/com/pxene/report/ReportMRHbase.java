package com.pxene.report;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class ReportMRHbase {
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene03,pxene04");
	static Logger log = Logger.getLogger(ReportMRHbase.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {	
		
		String tablename = "dsp_tanx_usefull";        
//        creatTable(tablename,new String[]{"t","mdid","cg"});  
        creatTable(tablename,"br"); 
        
        String src_table_name="dsp_tanx_bidrequest_log";
         HTable    distTable=   new HTable(conf, tablename);
        conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);          
        
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();      
//        String outPath = otherArgs[otherArgs.length-1];
		 
        Job job = new Job(conf,"tanx_usefull table");
        job.setJarByClass(ReportMRHbase.class);  
        job.setNumReduceTasks(3);  
        job.setMapperClass(Map.class);  
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(ImmutableBytesWritable .class);
        job.setOutputValueClass(Result.class);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
        job.setMapOutputValueClass(Result.class);  
                        
//        job.setInputFormatClass(TextInputFormat.class);  
//        job.setOutputFormatClass(TextOutputFormat.class);
		log.info("~~ set mapper table is dsp_tanx_bidrequest_log  and reducer table is dsp_tanx_usefull");
//	 	job.setNumReduceTasks(tasks);
//        Path o = new Path(new Path(outPath) +File.separator + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));       
//		FileOutputFormat.setOutputPath(job,o);	
//        HFileOutputFormat2.configureIncrementalLoad(job,distTable);
        
        Scan scan = new Scan();     
        scan.setTimeRange(1417609382670l, 1418609382670l);		
		TableMapReduceUtil.initTableMapperJob(src_table_name, scan, Map.class, ImmutableBytesWritable .class, Result.class, job);			
		TableMapReduceUtil.initTableReducerJob("dsp_tanx_usefull", Reduce.class, job);		
		
		
		 
		log.info("~~ Job configure complete  , waitForCompletion...");
		
		int jobResult = job.waitForCompletion(true)?0:1; 
//        
//		log.info("~~mapreducer success , current jobResult is "+ jobResult +" and HFile path is "+o);
//		
//       // HFile入库到HBase              
//		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
//		
//		log.info("~~ current hfile path is  "+o);		
//		
//		loader.doBulkLoad(o,  new HTable(conf, tablename));	
		
		System.exit(jobResult);
	}
	
	public static class Map extends TableMapper<ImmutableBytesWritable, Result> {   
		
		public void map(ImmutableBytesWritable  key, Result  value, Context context) throws IOException, InterruptedException {	  			 
			   
			   context.write(key, value);  
			   
//			   String[] values = value.toString().split(",");  			   
//			   
//			   List<KeyValue> list = new ArrayList<KeyValue>();
//					   
//			   ImmutableBytesWritable rowkey = new ImmutableBytesWritable(value.toString().split(",")[0].getBytes());
//			   			   
//			   for (int i = 0; i < values.length; i++) {
//				   String [] ss = values[i].toString().split("/");
//				    System.out.println("");
//				    String k = ss[0];  
//				    String [] f = ss[1].split(":");
//	                String family =f[0];  
//	                String qualifier = f[1];  
//	                String time =ss[2];
//	                String value_str =ss[5];
//	                
//	                KeyValue kv = new KeyValue(Bytes.toBytes(k),  
//	                        Bytes.toBytes(family), Bytes.toBytes(qualifier),  
//	                        Long.parseLong(time), Bytes.toBytes(value_str));
//	                
//	                list.add(kv);
//			   }		   	   
//			   Iterator<KeyValue> it = list.iterator();  
//	           while (it.hasNext()) {  
//	               KeyValue kv = new KeyValue();  
//	               kv = it.next();  
//	               if (kv != null) {  
//	                   context.write(rowkey, kv);  
//	               }  
//	           }  
		 }
	}
	
	public static class Reduce extends TableReducer<ImmutableBytesWritable,Result,ImmutableBytesWritable> {  
	
		@SuppressWarnings("deprecation")
		public void reduce(Text key,Iterable<KeyValue> value,Context context){
	        String k = key.toString();
	        log.info("~~ current k========="+k);
	        String time = k.substring(k.length()-13, k.length());
   		 	
	        log.info("~~ current time========="+time);
	        
	        Put putrow = new Put(k.getBytes());
	        putrow.add("br".getBytes(), Bytes.toBytes("t"), Bytes.toBytes(time));
	        
	        for (KeyValue t:value) {
	        	if(new String(t.getQualifier()).equals("mdid")){
	        		 putrow.add("br".getBytes(), Bytes.toBytes("mdid"), t.getValue());
	        		 
	        	}else if(new String(t.getQualifier()).equals("cg")){
	        		 putrow.add("br".getBytes(), Bytes.toBytes("cg"), t.getValue());
	        		 
	        	}
	        }
	        try {     
	            context.write(new ImmutableBytesWritable(key.getBytes()), putrow);
	            
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
	    }
	}
		
	@SuppressWarnings("deprecation")
	public static void creatTable(String tableName, String family){   
        HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			
			if (admin.tableExists(tableName)) { 
				 log.info("~~ table already exists!");
	            
	         } else {   
	             HTableDescriptor tableDesc = new HTableDescriptor(tableName);   
	             //for(int i=0; i<familys.length; i++){   
	                 tableDesc.addFamily(new HColumnDescriptor(family));   
	             //}   
	             admin.createTable(tableDesc);   
	             
	             log.info("~~ create table " + tableName + " ok.");  
	         }  
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
     } 
}
