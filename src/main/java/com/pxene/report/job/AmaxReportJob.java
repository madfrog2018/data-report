package com.pxene.report.job;

import com.pxene.report.AmaxReportMR;
import com.pxene.report.map.AmaxReportMapper;
import com.pxene.report.reduce.AmaxReportReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportJob {

    private static final Logger logger = Logger.getLogger(AmaxReportMR.class);

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
        QualifierFilter fi =new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("aid|acat|abundle"));
        scan.setFilter(fi);

        TableMapReduceUtil.initTableMapperJob(src_table_name, scan, AmaxReportMapper.AppAndCategoryMap.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(dist_table_name, AmaxReportReducer.SumReduce.class,job);

        logger.info("~~ Job configure complete  , waitForCompletion...");

        int jobResult = job.waitForCompletion(true) ? 0 : 1;
        logger.info("~~ job complete  status is "+ jobResult);

        return jobResult;

    }
}
