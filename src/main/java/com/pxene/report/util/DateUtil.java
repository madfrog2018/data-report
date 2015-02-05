package com.pxene.report.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	
	/**
	 * 根据根据给出的时间戳字符串 得出 所在周 周一0点 的时间戳字符串
	 * @param t
	 * @throws ParseException
	 */
	public String convertWeekByTime(String timestamp) throws ParseException {  			
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式           
        Date d =new Date(Long.parseLong(timestamp));   
        
        Calendar cal = Calendar.getInstance();  
        cal.setTime(sdf.parse(sdf.format(d)));  

        //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了  
        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
        if(1 == dayWeek) {  
           cal.add(Calendar.DAY_OF_MONTH, -1);  
        }  
//        System.out.println("要计算日期为:"+sdf.format(cal.getTime())); //输出要计算日期  
        
        cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，是星期一  
        int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天  
        cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值   
//        String imptimeBegin = sdf.format(cal.getTime());  
//        System.out.println("所在周星期一的日期："+imptimeBegin);  

//        cal.add(Calendar.DATE, 6);  
//        String imptimeEnd = sdf.format(cal.getTime());  
//        System.out.println("所在周星期日的日期："+imptimeEnd);  
        return Long.toString(cal.getTime().getTime());
	}  
	
	/**
	 * 根据根据给出的时间戳字符串 得出 所在月 1日0点 的时间戳字符串
	 * @param t
	 * @throws ParseException
	 */
	public String convertMonthByTime(String timestamp) throws ParseException {  			
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式           
        Date d =new Date(Long.parseLong(timestamp));       
        Date time = sdf.parse(sdf.format(d));    
        
        Calendar cal = Calendar.getInstance();  
        cal.setTime(time); 
        cal.set(Calendar.DAY_OF_MONTH, 1);
        
        return Long.toString(cal.getTime().getTime());
	}  
	
	/**
	 * 根据给出的时间戳字符串 得出 当天0点的时间戳字符串
	 * @throws ParseException 
	 */
	public String convertDayByTime(String timestamp) throws ParseException{
		
		SimpleDateFormat sf =new SimpleDateFormat("yyyy-MM-dd");	
		Date d =new Date(Long.parseLong(timestamp));
		
		return Long.toString(sf.parse(sf.format(d)).getTime());
	}
	
}
