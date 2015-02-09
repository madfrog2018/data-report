package com.pxene.report;

import com.pxene.report.job.AmaxReportJob;
import com.pxene.report.util.HBaseHelper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportMR extends Configured implements Tool{

    private static final Logger logger = Logger.getLogger(AmaxReportMR.class);
    private static Configuration conf = HBaseHelper.getHBConfig(
            "pxene01,pxene02,pxene03,pxene04,pxene05");



    @Override
    public int run(String[] args) throws Exception {

        Options opts = new Options();
        Option amaxAppAndCategoryOpt = OptionBuilder.withArgName("amaxAppAndCategory").hasArg()
                .withDescription("get amax app and category info").create("amaxAppAndCategoryOpt");

        Option amaxSrcTableNameOpt = OptionBuilder.withArgName("amaxSrcTableName").hasArg()
                .withDescription("amax app and category src table name").create("amaxSrcTableNameOpt");

        Option amaxDistTableNameOpt = OptionBuilder.withArgName("amaxDistTableName").hasArg()
                .withDescription("amax app and category dist table name").create("amaxDistTableNameOpt");

        opts.addOption(amaxAppAndCategoryOpt);
        opts.addOption(amaxSrcTableNameOpt);
        opts.addOption(amaxDistTableNameOpt);

        String formatstr = "AmaxReportMR --amaxAppAndCategory --amaxSrcTableName --amaxDistTableName";
        CommandLineParser parser = new PosixParser();
        CommandLine cl =null;
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(formatstr, opts);

        try {
            cl = parser.parse(opts, args);
        } catch (Exception e) {

            logger.error("error" + e.getMessage());
        }
        if (cl.hasOption("amaxAppAndCategory")) {
            String srcTableName = cl.getOptionValue("amaxSrcTableName");
            String distTableName = cl.getOptionValue("amaxDistTableName");
            int appCategoryResult = AmaxReportJob.AppAndCategoryJob(conf, srcTableName, distTableName);
            return appCategoryResult;
        }
        return 0;
    }
}
