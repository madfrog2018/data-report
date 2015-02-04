package com.pxene.report.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class Driver {
	
	public static Configuration config(){
		Configuration config = HBaseHelper.getDefaultHBConfig();
		config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		config.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
				config.set("mapreduce.task.io.sort.mb", "512");
		config.setInt("mapreduce.task.io.sort.factor", 100);
		config.setInt("mapreduce.reduce.shuffle.parallelcopies", 50);// default// 5
																	// 426293
		config.setBoolean("mapreduce.map.output.compress", true);
//		config.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		config.set("dfs.blocksize", "128m");// block 128M
		config.set("io.file.buffer.size", "131072");// default 4k
		config.setBoolean("mapreduce.map.speculative", false);
		config.setBoolean("mapreduce.reduce.speculative", false);
		config.setInt("min.num.spill.for.combine", 50);
		config.setInt("mapreduce.job.maps", 100);
		config.setBoolean("hbase.regionserver.restart.on.zk.expire", true);
		config.set("hbase.regionserver.lease.period", "10000000");
		config.setInt("mapreduce.reduce.merge.inmem.threshold", 0);
		config.set("mapred.job.reduceinput.buffer.percent", "1.0");
		config.set("hbase.zookeeper.property.maxClientCnxns", "500");
		config.set("mapred.jobtracker.taskScheduler", "org.apache.hadoop.mapred.FairScheduler");
		return config;
	}
}
