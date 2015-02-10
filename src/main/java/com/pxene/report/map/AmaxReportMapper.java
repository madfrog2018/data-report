package com.pxene.report.map;

import com.pxene.report.util.DBUtil;
import com.pxene.report.util.DateUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportMapper {

    private static final Logger logger = Logger.getLogger(AmaxReportMapper.class);
    private final static Character cgseparator = 0x01;
    private final static String rowkeyseparator = ";";
    private final static String week = "week";
    private final static String month = "month";
    static DBUtil db = new DBUtil();
    static DateUtil dateUtil  = new DateUtil();

    /**
     * app和分类对应关系
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

            List<KeyValue> list = value.list();

            String pidValue = "";
            String mpnValue = "";
            List<String> cgValue = new ArrayList<>();
            for (KeyValue kv : list) {

                String qu = Bytes.toString(kv.getQualifier());
                switch (qu) {
                    case "acat":
                        String cgV = Bytes.toString(kv.getValue());
                        if (cgV.contains(String.valueOf(cgseparator))) {

                            Arrays.asList(qu.split(String.valueOf(cgseparator)));
                        } else {
                            cgValue.add(cgV);
                        }
                        break;
                    case "aid":
                        pidValue = Bytes.toString(kv.getValue());
                        break;
                    case "abundle":
                        mpnValue = Bytes.toString(kv.getValue());
                        break;
                }
            }
            for (String s : cgValue) {

                if (s !=null && s.trim().length() > 0) {
                    Text resultKey = new Text();
                    resultKey.set(pidValue + rowkeyseparator + s + rowkeyseparator + mpnValue);
                    context.write(resultKey,one);
                }
            }

        }
    }


    /**
     * 数据dsp_amax_app_category插入到mysql dsp_t_amax_app_category表里
     */
    public static class SaveToMysql_appcategoryMap extends TableMapper<Text, IntWritable>{

        @Override
        protected void map(
                ImmutableBytesWritable key,
                Result value,
                Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            String rowkey = Bytes.toString(key.get()).trim();
            String [] data = rowkey.split(rowkeyseparator, -1);

            db.insertToAppcategory(data[0].trim(),data[1].trim(), data[2].trim(), "dsp_t_amax_app_category");

        }
    }

    /**
     * 每天 每个appid 出现使用的次数
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
            String pidValue = "";
            for (KeyValue kv : value.list()) {
                String qu = Bytes.toString(kv.getQualifier());
                if(qu.equals("aid")){
                    pidValue = Bytes.toString(kv.getValue());
                }
            }

            if(rowKey.length()>32){
                String time = rowKey.substring(32, rowKey.length()).trim();
                try {
                    if(time.matches("[0-9]+")){
                        Text resultKey = new Text();
                        resultKey.set(dateUtil.convertDayByTime(time)+rowkeyseparator+pidValue);
                        context.write(resultKey,one);
                    }
                } catch (ParseException e) {
                    logger.error("parse exception is " + e.getMessage());
                }
            }
        }
    }

    /**
     * 数据dsp_tanx_appused_count插入到mysql dsp_t_amax_app_used_count表里
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

            db.insertToAppusedCount(Long.parseLong(data[0].trim()),data[1].trim(), Integer.valueOf(count),
                    "dsp_t_amax_app_used_count");
        }
    }


    /**
     * 每周/月  访问每个app的人
     *key:time;week/month;pid;mdid
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
                if(qu.equals("aid")){
                    pidValue = Bytes.toString(kv.getValue());
                }else if(qu.equals("did") || qu.equals("mac")){
                    mdidValue = Bytes.toString(kv.getValue());
                }
            }

            if (rowKey.length() > 32) {
                String time = rowKey.substring(32, rowKey.length());
                try {
                    if(time.matches("[0-9]+")){
                        //周
                        Text resultKey = new Text();
                        resultKey.set(dateUtil.convertWeekByTime(time)+rowkeyseparator+week+
                                rowkeyseparator+pidValue+rowkeyseparator+mdidValue);
                        context.write(resultKey,one);

                        //月
                        Text resultKey2 = new Text();
                        resultKey2.set(dateUtil.convertMonthByTime(time) + rowkeyseparator + month +
                                rowkeyseparator + pidValue + rowkeyseparator + mdidValue);
                        context.write(resultKey2,one);

                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 1.每周/月  访问每个app的人数
     *key:time;week/month;pid
     *value:1
     *return：sum
     */
    public static class DeviceIdByTime_CountMap extends TableMapper<Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(
                ImmutableBytesWritable key,
                Result value,
                Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            String [] keys = (Bytes.toString(key.get())).split(rowkeyseparator, -1);

            Text resultKey = new Text();
            resultKey.set(keys[0]+rowkeyseparator+keys[1]+rowkeyseparator+keys[2]);
            context.write(resultKey,one);
        }
    }

    /**
     * 数据dsp_tanx_deviceId_count插入到mysql dsp_t_amax_app_deviceId_count表里
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

            db.insertToDeviceIdCount(Long.parseLong(data[0].trim()),data[2].trim(),
                    Integer.valueOf(count),data[1].trim(), "dsp_t_amax_app_deviceId_week_count",
                    "dsp_t_amax_app_deviceId_month_count");
        }
    }


    /**
     * (app使用天，日app使用人）
     *rowkey = time(一天的0点);appid;mdid
     *同app，同用户，
     *同一天去重复次数
     */
    public static class DistinctByDay_Map extends TableMapper<Text, IntWritable> {
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

                if(qu.equals("aid")){
                    pidValue = Bytes.toString(kv.getValue());
                }else if(qu.equals("did") || qu.equals("mac")){
                    mdidValue = Bytes.toString(kv.getValue());
                }
            }
            if(rowKey.length()>32){
                String time = rowKey.substring(32, rowKey.length());
                try {
                    if(time.matches("[0-9]+")){
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
     * app使用天数
     *rowkey = time(周一的0点);week/month;appid
     *合并出一周 使用同一个app的个数 ，即是天数
     */
    public static class AppUsed_CountDays_Map extends TableMapper<Text, IntWritable> {
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
                if(keys[0].matches("[0-9]+")){
                    //周
                    Text resultKey = new Text();
                    resultKey.set(dateUtil.convertWeekByTime(keys[0])+rowkeyseparator+week+rowkeyseparator+keys[1]);
                    context.write(resultKey,one);

                    //月
                    Text resultKey2 = new Text();
                    resultKey2.set(dateUtil.convertMonthByTime(keys[0])+rowkeyseparator+month+rowkeyseparator+keys[1]);
                    context.write(resultKey2,one);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 数据dsp_tanx_deviceId_count插入到mysql dsp_t_app_deviceId_count表里
     */
    public static class ConvertToMysql_useddayscountMap extends TableMapper<Text, IntWritable>{

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

            db.insertToUsedDaysCount(Long.parseLong(data[0].trim()),data[2].trim(), Integer.valueOf(count),data[1].trim());
        }
    }

    /**日使用人数
     *rowkey = time(每天的0点);appid
     *合并出一天 使用同一个app的个数 ，即是人数
     */
    public static class DeviceIdByDay_CountMap extends TableMapper<Text, IntWritable>{
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
     * 数据dsp_tanx_deviceIdByDay_count插入到mysql dsp_t_app_deviceIdByDay_count表里
     */
    public static class ConvertToMysql_deviceIdcountbydayMap extends TableMapper<Text, IntWritable>{

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

            db.insertToDeviceIdByDayCount(Long.parseLong(data[0].trim()),data[1].trim(), Integer.valueOf(count));
        }
    }
}
