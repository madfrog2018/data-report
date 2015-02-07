package com.pxene.report.map;

import com.pxene.report.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by young on 2015/2/7.
 */
public class DeleteRowsMapper extends TableMapper<Text, IntWritable>{

    private static final Logger logger = Logger.getLogger(DeleteRowsMapper.class);
    private static Configuration conf = HBaseHelper.getHBConfig(
            "pxene01,pxene02,pxene03,pxene04,pxene05");

//    private static HTable htable = HBaseHelper.getHelper(conf).getCon().getTable("dsp_tanx_bidreuqest_log");


    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException, InterruptedException {

        List<Cell> cells = value.listCells();

        for (Cell cell : cells) {

            byte[] rowkey = cell.getRowArray();
//            Delete delete = new Delete(rowkey);


        }

    }
}
