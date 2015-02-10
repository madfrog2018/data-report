package com.pxene.report.job;

import com.pxene.report.AmaxReportMR;
import com.pxene.report.map.AmaxReportMapper;
import com.pxene.report.map.UserMapper;
import com.pxene.report.reduce.AmaxReportReducer;
import com.pxene.report.reduce.UserReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportJob {

    private static final Logger logger = LogManager.getLogger(AmaxReportMR.class);

    /**
     * 获取 rowkey = appId-appcategory-package 存到中间表里
     * @param src_table_name ：原表
     * @param dist_table_name ：目标表 dsp_amax_app_category
     * @return
     * @throws Exception
     */
    public static int AppAndCategoryJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{

        Job job = Job.getInstance(conf, "dsp_amax_app_category table Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(AmaxReportMapper.AppAndCategoryMap.class);
        job.setReducerClass(AmaxReportReducer.SumReduce.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("aid|acat|abundle"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.AppAndCategoryMap.class,
                Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, AmaxReportReducer.SumReduce.class,job);

        logger.info("~~ Job configure complete  , waitForCompletion...");

        int jobResult = job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ job complete  status is "+ jobResult);

        return jobResult;

    }

    /**
     * 取出hbase中dsp_amax_app_category表数据，导入到mysql里
     */
    public static int SaveToMysql_appcategoryJob (Configuration conf,String src_table_name) throws Exception{
        Job job = Job.getInstance(conf, "save data To Mysql Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(AmaxReportMapper.SaveToMysql_appcategoryMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        scan.setFilter(fi);
        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.SaveToMysql_appcategoryMap.class,
                Text.class, IntWritable.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 每天app被访问的次数
     * 目标表：dsp_amax_appused_count
     */
    public static int AppUsedCountJob(Configuration conf,String src_table_name,String dist_table_name) throws Exception{
        Job job = Job.getInstance(conf, "dsp_amax_appused_count table Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(AmaxReportMapper.AppUsedCountMap.class);
        job.setReducerClass(AmaxReportReducer.SumReduce.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("aid"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.AppUsedCountMap.class,
                Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, AmaxReportReducer.SumReduce.class,job);

        logger.info("~~ Job configure complete  , waitForCompletion...");

        int jobResult = job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ job complete  status is "+ jobResult);

        return jobResult;

    }

    /**
     * 取出hbase中dsp_amax_appused_count表数据，导入到mysql里
     */
    public static int ConvertToMysql_appusedcountJob (Configuration conf,String src_table_name) throws Exception{
        Job job = Job.getInstance(conf, "Convert data To Mysql Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(AmaxReportMapper.ConvertToMysql_appusedcountMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        scan.setFilter(fi);
        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.ConvertToMysql_appusedcountMap.class,
                Text.class, IntWritable.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * 每周 /月 访问app的人数
     * 中间去重表 ： dsp_amax_deviceId_distinct
     * 目标表：dsp_amax_deviceId_count
     */
    public static int DeviceIdCountJob(Configuration conf,String src_table_name,String middle_table_name,
                                       String dist_table_name) throws Exception{
        //周/月   distinct
        Job distinct_job = Job.getInstance(conf, "dsp_amax_deviceId_distinct_count table Job");
        distinct_job.setJarByClass(AmaxReportMR.class);
        distinct_job.setNumReduceTasks(3);
        distinct_job.setMapperClass(AmaxReportMapper.DeviceIdCountMap.class);
        distinct_job.setReducerClass(AmaxReportReducer.DistinctReduce.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("aid|did|mac"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.DeviceIdCountMap.class,
                Text.class, IntWritable.class, distinct_job);
        TableMapReduceUtil.initTableReducerJob(middle_table_name, AmaxReportReducer.DistinctReduce.class, distinct_job);

        logger.info("~~ jobResult configure complete  , waitForCompletion...");

        int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);


        //周/月 count
        Job count_job = Job.getInstance(conf, "dsp_amax_deviceId_count table Job");
        count_job.setJarByClass(AmaxReportMR.class);
        count_job.setNumReduceTasks(3);
        count_job.setMapperClass(AmaxReportMapper.DeviceIdByTime_CountMap.class);
        count_job.setReducerClass(AmaxReportReducer.SumReduce.class);

        Scan week_scan = new Scan();
        QualifierFilter fit =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        week_scan.setFilter(fit);

        TableMapReduceUtil.initTableMapperJob(middle_table_name, week_scan,
                AmaxReportMapper.DeviceIdByTime_CountMap.class,Text.class, IntWritable.class, count_job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, AmaxReportReducer.SumReduce.class,count_job);

        logger.info("~~ jobResult configure complete  , waitForCompletion...");

        int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ count_jobResult complete  status is "+ count_jobResult);

        return count_jobResult;
    }

    /**
     * 取出hbase中dsp_amax_deviceId_count表数据，导入到mysql里
     */
    public static int ConvertToMysql_deviceIdcountJob (Configuration conf,String src_table_name) throws Exception{
        Job job = Job.getInstance(conf, "Convert data To Mysql Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(AmaxReportMapper.ConvertToMysql_deviceIdcountMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        scan.setFilter(fi);
        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.ConvertToMysql_deviceIdcountMap.class,
                Text.class, IntWritable.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * 每周/月 访问app的总天数
     * 中间去重表 ： dsp_amax_usedDay_distinct
     * 目标表：dsp_amax_usedDay_count
     */
    public static int AppUsedDaysCountJob(Configuration conf,String src_table_name,String middle_table_name,String dist_table_name) throws Exception{
        //周/月  distinct(去重复 同一个人 同一天 使用同一个app 记成一条记录)
        Job distinct_job = Job.getInstance(conf, "dsp_amax_distinct_days table Job");
        distinct_job.setJarByClass(AmaxReportMR.class);
        distinct_job.setNumReduceTasks(3);
        distinct_job.setMapperClass(AmaxReportMapper.DistinctByDay_Map.class);
        distinct_job.setReducerClass(AmaxReportReducer.DistinctReduce.class);

        Scan scan = new Scan();
        QualifierFilter fi = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("aid|did|mac"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.DistinctByDay_Map.class,Text.class, IntWritable.class, distinct_job);
        TableMapReduceUtil.initTableReducerJob(middle_table_name, AmaxReportReducer.DistinctReduce.class,distinct_job);

        logger.info("~~ distinct_job configure complete  , waitForCompletion...");

        int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);


//		周/月  count(合并出一周 使用同一个app的个数 ，即是天数)
        Job count_job = Job.getInstance(conf, "dsp_tanx_days_count table Job");
        count_job.setJarByClass(AmaxReportMR.class);
        count_job.setNumReduceTasks(3);
        count_job.setMapperClass(AmaxReportMapper.AppUsed_CountDays_Map.class);
        count_job.setReducerClass(AmaxReportReducer.SumReduce.class);

        Scan count_scan = new Scan();
        QualifierFilter fit =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        count_scan.setFilter(fit);

        TableMapReduceUtil.initTableMapperJob(middle_table_name, count_scan,
                AmaxReportMapper.AppUsed_CountDays_Map.class,Text.class, IntWritable.class, count_job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, AmaxReportReducer.SumReduce.class,count_job);

        logger.info("~~ count_job configure complete  , waitForCompletion...");

        int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ count_job complete  status is "+ count_jobResult);

        return count_jobResult;
    }

    /**
     * 取出hbase中dsp_tanx_usedDay_count表数据，导入到mysql里
     */
    public static int ConvertToMysql_useddayscountJob (Configuration conf,String src_table_name) throws Exception{
        Job job = Job.getInstance(conf, "Convert data To Mysql Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(UserMapper.ConvertToMysql_useddayscountMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        scan.setFilter(fi);
        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.ConvertToMysql_useddayscountMap.class, Text.class, IntWritable.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * 日使用人数
     * 中间去重表 ： dsp_tanx_deviceIdByDay_distinct
     * 目标表：dsp_tanx_deviceIdByDay_count
     */
    public static int DeviceIdCountByDayJob(Configuration conf,String src_table_name,String middle_table_name,String dist_table_name) throws Exception{
        //日人数  distinct(去重复 同一个人 同一天 使用同一个app 记成一条记录)
        Job distinct_job = Job.getInstance(conf, "dsp_tanx_distinct_days table Job");
        distinct_job.setJarByClass(AmaxReportMR.class);
        distinct_job.setNumReduceTasks(3);
        distinct_job.setMapperClass(UserMapper.DistinctByDay_Map.class);
        distinct_job.setReducerClass(UserReducer.DistinctReduce.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("pid|mdid"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.DistinctByDay_Map.class,Text.class, IntWritable.class, distinct_job);
        TableMapReduceUtil.initTableReducerJob(middle_table_name, UserReducer.DistinctReduce.class,distinct_job);

        logger.info("~~ distinct_job configure complete  , waitForCompletion...");

        int distinct_jobResult = distinct_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ distinct_jobResult complete  status is "+ distinct_jobResult);


//		日  count(合并出一天 使用同一个app的个数 ，即是人数)
        Job count_job = Job.getInstance(conf, "dsp_tanx_days_count table Job");
        count_job.setJarByClass(AmaxReportMR.class);
        count_job.setNumReduceTasks(3);
        count_job.setMapperClass(UserMapper.DeviceIdByDay_CountMap.class);
        count_job.setReducerClass(UserReducer.SumReduce.class);

        Scan count_scan = new Scan();
        QualifierFilter fit =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        count_scan.setFilter(fit);

        TableMapReduceUtil.initTableMapperJob(middle_table_name, count_scan, UserMapper.DeviceIdByDay_CountMap.class,Text.class, IntWritable.class, count_job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, UserReducer.SumReduce.class,count_job);

        logger.info("~~ count_job configure complete  , waitForCompletion...");

        int count_jobResult = count_job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ count_job complete  status is "+ count_jobResult);

        return distinct_jobResult;
    }

    /**
     * 取出hbase中dsp_tanx_deviceIdByDay_count表数据，导入到mysql里
     */
    public static int ConvertToMysql_deviceIdCountByDayJob (Configuration conf,String src_table_name) throws Exception{
        Job job = Job.getInstance(conf, "Convert data To Mysql Job");
        job.setJarByClass(AmaxReportMR.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(UserMapper.ConvertToMysql_deviceIdcountbydayMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("count"));
        scan.setFilter(fi);
        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, UserMapper.ConvertToMysql_deviceIdcountbydayMap.class, Text.class, IntWritable.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
