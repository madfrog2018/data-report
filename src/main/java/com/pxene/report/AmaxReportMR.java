package com.pxene.report;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportMR extends Configured implements Tool{

    private static final Logger logger = Logger.getLogger(AmaxReportMR.class);




    @Override
    public int run(String[] args) throws Exception {

        int argsLength = args.length;
        if (argsLength < 2) {

            logger.error("please give the necerssary args");
            System.exit(-1);
        }
        String tableName = args[argsLength -1];


        return 0;
    }
}
