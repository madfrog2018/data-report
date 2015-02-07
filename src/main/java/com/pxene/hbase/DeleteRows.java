package com.pxene.hbase;


import com.pxene.report.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by young on 2015/2/6.
 */
public class DeleteRows {

    private static final Logger logger = LogManager.getLogger(DeleteRows.class);

    private static Configuration conf = HBaseHelper.getHBConfig(
            "pxene01,pxene02,pxene03,pxene04,pxene05");


    public static void main(String[] args) {

        int argsLength = args.length;
        if (argsLength < 1) {

            logger.error("please give the necessary args");
            System.exit(-1);
        }
        String tableName = args[argsLength - 1];

        batchDeleteByRow(tableName);
    }

    public static void batchDeleteByRow(String tablename){
        try {
            HTable table = new HTable(conf,tablename);
            Get get = new Get("0a622824000054ad3ebc4f1d0066970a1420639932378".getBytes());
            Result result = table.get(get);
            List<KeyValue> list = result.list();
            for (KeyValue keyValue : list) {

//                logger.info("key is " + keyValue.getKeyString() + "value is " + keyValue.getValueArray().toString());

            }
            logger.info(new String(result.getRow()));

//            Scan s = new Scan();
//            Filter filter = new PrefixFilter(String.valueOf(0x02).getBytes());
////          Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("\0x02".getBytes()));
//
//            s.setFilter(filter);
////            s.setStartRow((String.valueOf(0x02) + "1418628093267").getBytes());
////            s.setStopRow((String.valueOf(0x02) + "1418728573275").getBytes());
//            ResultScanner rs = table.getScanner(s);
//            List<Delete> list = new ArrayList<>();
//            for (Result re : rs) {
//                Delete d1 = new Delete(re.getRow());
//                logger.info("add delete item");
//                list.add(d1);
//            }
//            table.delete(list);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
