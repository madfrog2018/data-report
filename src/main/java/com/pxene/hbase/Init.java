package com.pxene.hbase;

import com.pxene.report.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/** 
 *  //过滤器
    //1、FilterList代表一个过滤器列表
	    //FilterList.Operator.MUST_PASS_ALL -->and
	    //FilterList.Operator.MUST_PASS_ONE -->or
	    //eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    //2、SingleColumnValueFilter
    //3、ColumnPrefixFilter用于指定列名前缀值相等
    //4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
    //5、QualifierFilter是基于列名的过滤器。
    //6、RowFilter
    //7、RegexStringComparator是支持正则表达式的比较器。
    //8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。
 *
 */
public class Init {
	private static Configuration conf = HBaseHelper.getHBConfig("pxene01,pxene02,pxene03,pxene04,pxene05");
//	private static Configuration conf = HBaseHelper.getHBConfig("slave2,slave1,master");
	
	
	
	public static void main(String[] args) throws Exception {
		
//		char a = 0x09;
//		System.out.println("====="+a+"====");
//		new Init().init();		
		
		String sd="2014-12-21 00:00:00"; 
		String sd2="2015-01-05";
		//3600000
//		Date date =new Date(1420041600000l);1420074000000
		Date date2 =new Date(1417449600000l);
		SimpleDateFormat sf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sf2 =new SimpleDateFormat("yyyy-MM-dd");
		System.out.println(sf.parse(sd).getTime()+"\n"+sf2.parse(sd2).getTime()); 
		
		String t = "1420041600000";
		Date d =new Date(Long.parseLong(t));		
		long milltime = sf.parse(sf.format(date2)).getTime();
//		System.out.println(d+","+sf2.format(d));//Sat Jan 24 16:18:23 CST 2015
		
		Long lg = 1418628093267l;
//		Long lg =  0a67f5230000547f00a60366001e5a631417609382670l;//dsp_tanx_bidrequest_log
		Character ct = 0x02;
		StringBuilder s = new StringBuilder();
		s.append(ct).append(lg);
		
		
		String rowKey ="0a67f52d000054c83e09484c0064b66f1422409225939";// "1422246914417";//
//		System.out.println(rowKey.substring(32, rowKey.length()));
//	  	creatTable("testn","br");
//	  	addRecord("test_report", String.valueOf(1420089560000l), "br", new String [] {"cg"},new String [] {"60102"});
//	  	deleteTable("dsp_tanx_deviceId_distinct");
		
//		Character prefix = 0x02;
//		StringBuilder sb = new StringBuilder();
//		sb.append(prefix).append("1418628093267");
//    	deleteRow("dsp_tanx_bidrequest_log",sb.toString());  
    	
//		getAllRecord("test_report");
//	  	getRow("dsp_tanx_bidrequest_log",rowKey);  
//	  	QueryByCondition2("dsp_tanx_bidrequest_log");
//	  	QueryByCondition3("dsp_test_shs");
//		batchDeleteByRow("dsp_tanx_bidrequest_log");
//	  	getTanxColumn();
		
		
//		convertWeekByDate("1422087503570");
	}
	
	public static void convertWeekByDate(String timestamp) throws ParseException {  	    		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式           
        Date d =new Date(Long.parseLong(timestamp));       
        Date time = sdf.parse(sdf.format(d));    
        
        Calendar cal = Calendar.getInstance();  
        cal.setTime(time);  
        
        cal.set(Calendar.DAY_OF_MONTH, 1);
//        //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了  
//        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
//        if(1 == dayWeek) {  
//           cal.add(Calendar.DAY_OF_MONTH, -1);  
//        }  
////        System.out.println("要计算日期为:"+sdf.format(cal.getTime())); //输出要计算日期  
//
//        cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一  
//        int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
//        cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值   

        String imptimeBegin = sdf.format(cal.getTime());  
        System.out.println(cal.getTime().getTime()+"所在周星期一的日期："+imptimeBegin);  

//        cal.add(Calendar.DATE, 6);  
//        String imptimeEnd = sdf.format(cal.getTime());  
//        System.out.println("所在周星期日的日期："+imptimeEnd);  
	}  
	
	 /** 
     * 创建一张表 
     */   
	public static void creatTable(String tableName, String family){   
         HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			
			if (admin.tableExists(tableName)) {   
	             System.out.println("table already exists!");   
	         } else {   
	             HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));   
	                
	             tableDesc.addFamily(new HColumnDescriptor(family));   
	            
	             admin.createTable(tableDesc);   
	             System.out.println("create table " + tableName + " ok.");   
	         }  
		} catch (Exception e) {
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
			e.printStackTrace();
		}  
      }    
      
      /** 
       * 插入一行记录 
       */   
       @SuppressWarnings("resource")
	public static void addRecord (String tableName, String rowKey, String family, String [] qualifier, String [] value){   
           try {   
               HTable table = new HTable(conf, tableName);   
               Put put = new Put(Bytes.toBytes("0a67f5230000547f00a60366001e5a63"+rowKey));   
               // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  
              for (int i = 0; i < qualifier.length; i++) {
            	  put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier[i]),Bytes.toBytes(value[i]));  
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
     * 根据rowkey,过滤 批量删除  
     * 
     * 提取rowkey以01结尾数据
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*01$"));
		
		提取rowkey以包含201407的数据
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("201407"));
			
		提取rowkey以123开头的数据
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("123".getBytes()));
     */
      @SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void batchDeleteByRow(String tablename){
	   	   try {
				HTable table = new HTable(conf,tablename);
				
				Scan s = new Scan();
//				Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("\0x02".getBytes()));
//				s.setFilter(filter);

//				s.setStartRow()
				ResultScanner rs = table.getScanner(s);
				List list = new ArrayList();
				for (Result re : rs) {
		            Delete d1 = new Delete(re.getRow());  
		            list.add(d1);  
				}
				table.delete(list);
			} catch (IOException e) {
				e.printStackTrace();
			}
      }
      
      
    /**
	 * 通过rowkey获取数据
	 */
	@SuppressWarnings({"resource", "deprecation" })
	public static void getRow(String tablename,String rowkey) throws IOException {		
		HBaseHelper Hhelper = HBaseHelper.getHelper(conf);
		try {
			HTable table = new HTable(conf, tablename); 
			 
            Get get = new Get(rowkey.getBytes());   
            Result rs = table.get(get);  
            System.out.println(rs.size());
            for(Cell kv : rs.listCells()){   
                System.out.print(Hhelper.getStringFromBytes(kv.getRow()) + " " );   //rowley
                System.out.print(Hhelper.getStringFromBytes(kv.getFamilyArray()) + ":" );   //br
                System.out.print(Hhelper.getStringFromBytes(kv.getQualifierArray()) + " " );   //column
                System.out.print(kv.getTimestamp() + " " );   
                System.out.println(Hhelper.getStringFromBytes(kv.getValue()));   //value
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
	@SuppressWarnings({ "resource", "deprecation" })
	public static void getAllRecord (String tableName) {   
         try{   
             HTable table = new HTable(conf, tableName);   
             Scan s = new Scan(); 
//             s=HBaseHelper.setTimeRang(s,start,end);
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
    @SuppressWarnings({ "resource", "deprecation" })
	public static void QueryByCondition2(String tableName) {  
  
        try {  
            HTablePool pool = new HTablePool(conf, 1000);  
            HTableInterface  table = (HTableInterface) pool.getTable(tableName);  
            Filter filter = new SingleColumnValueFilter(Bytes  
                    .toBytes("br"), Bytes.toBytes("mdid"), CompareOp.EQUAL, Bytes  
                    .toBytes("AQ/u+gFpNPLcd8iRYNwtg")); // 当列column1的值为aaa时进行查询  
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
    @SuppressWarnings({"resource", "deprecation" })
	public static void QueryByCondition3(String tableName) {  
  
        try {  
            HTablePool pool = new HTablePool(conf, 1000);  
            HTableInterface table = (HTableInterface) pool.getTable(tableName);  
  
            List<Filter> filters = new ArrayList<Filter>();  
  
            Filter filter1 = new SingleColumnValueFilter(Bytes  
                    .toBytes("br"), Bytes.toBytes("name"), CompareOp.EQUAL, Bytes  
                    .toBytes("test"));  
            filters.add(filter1);  
  
            Filter filter2 = new SingleColumnValueFilter(Bytes  
                    .toBytes("br"), Bytes.toBytes("end"), CompareOp.EQUAL, Bytes  
                    .toBytes("e4"));  
            filters.add(filter2);  
  
            Filter filter3 = new SingleColumnValueFilter(Bytes  
                    .toBytes("br"), Bytes.toBytes("start"), CompareOp.EQUAL, Bytes  
                    .toBytes("s4"));  
            filters.add(filter3);  
            
            //or
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,filters);              
            Scan scan = new Scan();  
            scan.setFilter(filterList);  

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
