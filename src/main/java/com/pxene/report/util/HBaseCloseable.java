package com.pxene.report.util;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class HBaseCloseable {
	
	public static final Bulider Bulider = new Bulider();

	private HTableInterface tableInterface;

	private ResultScanner scanner;
	
	private Result result;
	private HConnection connection;

	private HBaseCloseable(){}
	
	public  static class Bulider{
		private static final HBaseCloseable closeable=new HBaseCloseable();
		public Bulider addTableInterface(HTableInterface tableInterface){
			closeable.setTableInterface(tableInterface);
			return this;
		}
		public Bulider addScanner(ResultScanner scanner){
			closeable.setScanner(scanner);
			return this;
		}
		public Bulider addResult(Result result){
			closeable.setResult(result);
			return this;
		}
		public Bulider addHConnection(HConnection con){
			closeable.setConnection(con);
			return this;
		}
		public HBaseCloseable build(){
			return closeable;
		}
	}
	public HConnection getConnection() {
		return connection;
	}

	public void setConnection(HConnection connection) {
		this.connection = connection;
	}

	public Result getResult() {
		return result;
	}

	public HBaseCloseable setResult(Result result) {
		this.result = result;
		return this;
	}

	public HTableInterface getTableInterface() {
		return tableInterface;
	}

	public HBaseCloseable setTableInterface(HTableInterface tableInterface) {
		this.tableInterface = tableInterface;
		return this;
	}

	public ResultScanner getScanner() {
		return scanner;
	}

	public HBaseCloseable setScanner(ResultScanner scanner) {
		this.scanner = scanner;
		return this;
	}
	
}
