package com.pxene.report;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pxene.report.util.HBaseHelper;


public class Init {
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene03,pxene04");
//	private static Configuration conf = HBaseHelper.getHBConfig("slave2,slave1,master");
	
	public static void main(String[] args) throws IOException {
//		new Init().init();
		//0a67f5230000547f00a60366001e5a631417609382670
		//0a67f5230000547f00a60366001e5a631418929382670
//		System.out.println(Long.parseLong("1422008811328"));
		Date date =new Date(1417609382670l);
		SimpleDateFormat sf =new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		System.out.println(sf.format(date));
		System.out.println(String.valueOf(new Date().getTime()));
	
		Long lg = 1418628093267l;
//		Long lg =  0a67f5230000547f00a60366001e5a631417609382670l;//dsp_tanx_bidrequest_log
		Character ct = 0x02;
		StringBuilder s = new StringBuilder();
		s.append(ct).append(lg);
//		System.out.println(s);
		
		Long st = 1418628093267l;
		Long e  = 1418628093267l;
		
		String rowKey ="0a67f5230000547f00a60366001e5a631417609382670";// "1422246914417";//
//		System.out.println(rowKey.substring(rowKey.length()-13, rowKey.length()));
//	  	creatTable("dsp_tanx_usefull2",new String[]{"time","deviceid","appcode"});
//	  	addRecord("dsp_test_shs", String.valueOf(new Date().getTime()), new String[]{"start","name","end"}, "br",new String[]{"s4","test","e4"});
//	  	deleteTable("dsp_test_shs");
//    	deleteRow("dsp_test_shs",rowKey);  
//		getAllRecord("dsp_test_shs",st,e);
//	  	getRow("dsp_tanx_bidrequest_log",rowKey);  
//	  	QueryByCondition2("dsp_test_shs");
//	  	QueryByCondition3("dsp_test_shs");
//	  	getTanxColumn();
	}
	
	
	
//	public static class Map extends TableMapper<ImmutableBytesWritable, KeyValue> {   
//		
//		public void map(ImmutableBytesWritable  key, Result  value, Context context) throws IOException, InterruptedException {	   
//			   StringTokenizer itr = new StringTokenizer(value.toString());
//			   List<String> st = new ArrayList<String>();
//			   
//			   log.info("~~ current value========="+value);
//			   log.info("~~ current itr========="+itr);
//			   
//			   ImmutableBytesWritable rowkey = new ImmutableBytesWritable(value.toString().split(" ")[0].getBytes());
//			   
//			   log.info("~~ current rowkey========="+rowkey);
//			   
//			   List<KeyValue> list = new ArrayList<KeyValue>();
//			   int i = 0;
//			   while (itr.hasMoreTokens()) {
//				   if(itr.nextToken()!=null && itr.nextToken().length() > 0){
//					  st.add(itr.nextToken());
//					  //items[i] = itr.nextToken();
//					  log.info("~~ current items========="+st.get(i));
//					  i++;
//					  if(i > 3){
//						  if(st.get(3)!=null && st.size() > 3){
//							  String [] s = st.get(1).split(":");
//							  KeyValue kv = new KeyValue(Bytes.toBytes(st.get(0)),
//									   Bytes.toBytes(s[0]), Bytes.toBytes(s[1]),
//									   Long.parseLong(st.get(2).toString()), Bytes.toBytes(st.get(3)));
//							  list.add(kv);
//							 
//							  log.info("~~ current kv========="+kv);
//						  }
//						  i = 0;
//					  }
//				   }
//			   }
//			   	   
//			   Iterator<KeyValue> it = list.iterator();  
//	           while (it.hasNext()) {  
//	               KeyValue kv = new KeyValue();  
//	               kv = it.next();  
//	               if (kv != null) {  
//	                   context.write(rowkey, kv);  
//	               }  
//	           }  
//		 }
//	}
	
	/**
	 * 获取tanx表中的time= t ， deviceid（包括ios，android）= mdid ，appcode =cg ，数据放入到新建表"dsp_tanx_usefull"
	 */
	@SuppressWarnings({"resource", "rawtypes", "unchecked"})
	public static void getTanxColumn(){
		try {  
            
            HTable table = new HTable(conf, "dsp_tanx_bidrequest_log");   
            Scan s = new Scan();   
            ResultScanner ss = table.getScanner(s);   
            List list = new ArrayList();
            Map m = null;
            for(Result r:ss){   
                for(KeyValue kv : r.raw()){   
                	 String rowKey = new String(kv.getRow())+"";
                	
                	if(m!= null && m.get("rowKey").equals(rowKey)){
                		if(new String(kv.getQualifier()).equals("mdid")){
                    		m.put("deviceId", new String(kv.getValue()));
                    		
                    	}else if(new String(kv.getQualifier()).equals("cg")){
                    		m.put("appcode", new String(kv.getValue()));
                    		
                    	}
                	}else{
                		 m= new HashMap();
                		 //0a67f5230000547f00a60366001e5a631417609382670
                		
                		 String time = rowKey.substring(rowKey.length()-13, rowKey.length());
                		 m.put("rowKey", rowKey);
                		 m.put("time", Long.getLong(time));
                	}                 
                }   
                list.add(m);
            }
            ss.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
		
	}
	
	 /** 
     * 创建一张表 
     */   
	public static void creatTable(String tableName, String[] familys){   
         HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			
			if (admin.tableExists(tableName)) {   
	             System.out.println("table already exists!");   
	         } else {   
	             HTableDescriptor tableDesc = new HTableDescriptor(tableName);   
	             for(int i=0; i<familys.length; i++){   
	                 tableDesc.addFamily(new HColumnDescriptor(familys[i]));   
	             }   
	             admin.createTable(tableDesc);   
	             System.out.println("create table " + tableName + " ok.");   
	         }  
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
     } 
     
     /** 
      * 删除表 
      */   
      @SuppressWarnings("resource")
	public static void deleteTable(String tableName){   
        try {   
            HBaseAdmin admin = new HBaseAdmin(conf);   
            admin.disableTable(tableName);   
            admin.deleteTable(tableName);   
            System.out.println("delete table " + tableName + " ok.");   
        } catch (MasterNotRunningException e) {   
            e.printStackTrace();   
        } catch (ZooKeeperConnectionException e) {   
            e.printStackTrace();   
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
      }    
      
      /** 
       * 插入一行记录 
       */   
       @SuppressWarnings("resource")
	public static void addRecord (String tableName, String rowKey, String[] familys, String qualifier, String []values){   
           try {   
               HTable table = new HTable(conf, tableName);   
               Put put = new Put(Bytes.toBytes(rowKey));   
               for(int i=0; i<familys.length; i++){   
            	   put.add(Bytes.toBytes(familys[i]),Bytes.toBytes(qualifier),Bytes.toBytes(values[i]));                       
               }
               table.put(put);  
               System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");   
           } catch (Exception e) {   
               e.printStackTrace();   
           }   
      }  
       
   /** 
    * 删除一行记录 
    */   
    @SuppressWarnings({ "rawtypes", "resource", "unchecked" })
	public static void deleteRow(String tablename, String rowkey)  {  
        try {  
            HTable table = new HTable(conf, tablename);  
            List list = new ArrayList();  
            Delete d1 = new Delete(rowkey.getBytes());  
            list.add(d1);  
              
            table.delete(list);  
            System.out.println("del recored " + rowkey + " ok.");                
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
      
    /**
	 * 通过rowkey获取数据
	 */
	@SuppressWarnings({"resource" })
	public static void getRow(String tablename,String rowkey) throws IOException {			
		try {
			HTable table = new HTable(conf, tablename); 
			 
            Get get = new Get(rowkey.getBytes());   
            Result rs = table.get(get);  
            System.out.println(rs.size());
            for(KeyValue kv : rs.raw()){   
                System.out.print(new String(kv.getRow()) + " " );   //rowley
                System.out.print(new String(kv.getFamily()) + ":" );   //br
                System.out.print(new String(kv.getQualifier()) + " " );   //column
                System.out.print(kv.getTimestamp() + " " );   
                System.out.println(new String(kv.getValue()));   //value
            }  
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	/** 
     * 显示所有数据 
     * 1.统计出总设备数量
     * 遍历所有结果数据，new list，如果该key的值保存在，则new，否者，list.size+1
     */   
	@SuppressWarnings("resource")
	public static void getAllRecord (String tableName,long start,long end) {   
         try{   
             HTable table = new HTable(conf, tableName);   
             Scan s = new Scan(); 
             s=HBaseHelper.setTimeRang(s,start,end);
             ResultScanner ss = table.getScanner(s);   
             for(Result r:ss){   
                 for(KeyValue kv : r.raw()){   
                     System.out.print(new String(kv.getRow()) + " ");   
                     System.out.print(new String(kv.getFamily()) + ":");   
                     System.out.print(new String(kv.getQualifier()) + " ");   
                     System.out.print(kv.getTimestamp() + " ");   
                     System.out.println(new String(kv.getValue()));   
                 }   
             }   
         } catch (IOException e){   
             e.printStackTrace();   
         }   
     }  
     
	/** 
     * 单条件按查询，查询多条记录 
     */  
    @SuppressWarnings({ "resource" })
	public static void QueryByCondition2(String tableName) {  
  
        try {  
            HTablePool pool = new HTablePool(conf, 1000);  
            HTableInterface  table = (HTableInterface) pool.getTable(tableName);  
            Filter filter = new SingleColumnValueFilter(Bytes  
                    .toBytes("name"), Bytes.toBytes("br"), CompareOp.EQUAL, Bytes  
                    .toBytes("nihao")); // 当列column1的值为aaa时进行查询  
            Scan s = new Scan();  
            s.setFilter(filter);  
            ResultScanner rs = table.getScanner(s);  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====值:" + new String(keyValue.getValue()));  
                }  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
  
    } 
    
    /** 
     * 组合条件查询 
     * @param tableName 
     */  
    @SuppressWarnings({"resource" })
	public static void QueryByCondition3(String tableName) {  
  
        try {  
            HTablePool pool = new HTablePool(conf, 1000);  
            HTableInterface table = (HTableInterface) pool.getTable(tableName);  
  
            List<Filter> filters = new ArrayList<Filter>();  
  
            Filter filter1 = new SingleColumnValueFilter(Bytes  
                    .toBytes("name"), Bytes.toBytes("br"), CompareOp.EQUAL, Bytes  
                    .toBytes("test"));  
            filters.add(filter1);  
  
            Filter filter2 = new SingleColumnValueFilter(Bytes  
                    .toBytes("end"), Bytes.toBytes("br"), CompareOp.EQUAL, Bytes  
                    .toBytes("e4"));  
            filters.add(filter2);  
  
            Filter filter3 = new SingleColumnValueFilter(Bytes  
                    .toBytes("start"), Bytes.toBytes("br"), CompareOp.EQUAL, Bytes  
                    .toBytes("s4"));  
            filters.add(filter3);  
  
            FilterList filterList1 = new FilterList(filters);  
  
            Scan scan = new Scan();  
            scan.setFilter(filterList1);  
            ResultScanner rs = table.getScanner(scan);  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====值:" + new String(keyValue.getValue()));  
                }  
            }  
            rs.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
	
	
	
//	public void init() throws IOException {
//		HBaseHelper hbasehelper = hbasehelper.gethelper(hbasehelper
//				.gethbconfig("pxene01,pxene03,pxene04"));
//
//		hbasehelper.droptable("dmp_user");
//		hbasehelper.createtable("dmp_user", "name");
//	}
	
}
