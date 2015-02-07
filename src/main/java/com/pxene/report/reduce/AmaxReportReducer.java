package com.pxene.report.reduce;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by young on 2015/2/7.
 */
public class AmaxReportReducer {

    /**
     * reduce:sum
     * rowkey = appId;appcategory;package
     * rowkey = time;appId
     */
    public static class SumReduce extends TableReducer<Text, IntWritable, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> value,
                              Reducer<Text, IntWritable, Text, Mutation>.Context context)
                throws IOException, InterruptedException {

            //相同rowkey合并总是
            int sum = 0;
            for (IntWritable val : value) {
                sum += val.get();
            }
            result.set(sum);

            Put putrow = new Put(key.getBytes());
            String family = "br";
            putrow.add(Bytes.toBytes(family), Bytes.toBytes("count"), Bytes.toBytes(result.toString()));

            context.write(key, putrow);
        }

    }
}
