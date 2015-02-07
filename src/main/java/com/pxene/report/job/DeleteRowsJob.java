package com.pxene.report.job;


import com.pxene.report.DeleteRowsMR;
import com.pxene.report.map.DeleteRowsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

/**
 * Created by young on 2015/2/7.
 */
public class DeleteRowsJob {

    private static final Logger logger = Logger.getLogger(DeleteRowsJob.class);

    public static int deleteRows(Configuration conf, String tableName) throws Exception {

        Job deleteRowsJob = Job.getInstance(conf,"delete Rows");
        deleteRowsJob.setJarByClass(DeleteRowsMR.class);
        deleteRowsJob.setMapperClass(DeleteRowsMapper.class);
        deleteRowsJob.setOutputFormatClass(NullOutputFormat.class);

        Scan scan = new Scan();
        Filter filter = new PrefixFilter(String.valueOf(0x02).getBytes());
        scan.setFilter(filter);

        TableMapReduceUtil.initTableMapperJob(tableName, scan,DeleteRowsMapper.class,
                Text.class, IntWritable.class, deleteRowsJob);

        int jobResult = deleteRowsJob.waitForCompletion(true) ? 0 : 1;
        return jobResult;
    }
}
