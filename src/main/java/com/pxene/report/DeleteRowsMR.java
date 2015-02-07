package com.pxene.report;

import com.pxene.report.job.DeleteRowsJob;
import com.pxene.report.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by young on 2015/2/7.
 */
public class DeleteRowsMR extends Configured implements Tool{

    private static Configuration conf = HBaseHelper.getHBConfig(
            "pxene01,pxene02,pxene03,pxene04,pxene05");
    private static final Logger logger = Logger.getLogger(DeleteRowsMR.class);

    @Override
    public int run(String[] args) throws Exception {

        int argsLength = args.length;
        if (argsLength < 1) {

            logger.error("please give the necerssary args");
            System.exit(-1);
        }

        String tableName = args[argsLength -1];
        int jobResult = DeleteRowsJob.deleteRows(conf, tableName);
        return jobResult;
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new DeleteRowsMR(), args);
        } catch (Exception e) {
            logger.error("run error here is " + e.getMessage());
        }
    }
}
