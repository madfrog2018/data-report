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
}
