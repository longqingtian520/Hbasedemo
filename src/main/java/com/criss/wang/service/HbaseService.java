package com.criss.wang.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;

@Service
public class HbaseService {

	@Autowired
	private HbaseTemplate htemplate;

	/**
	 * 判断表是否存在
	 *
	 * @param tableName
	 * @return
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public boolean isExist(String tableName) throws IOException, ZooKeeperConnectionException, IOException {
		Configuration conf = htemplate.getConfiguration();
		Connection conn = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		return admin.tableExists(tableName);
	}

	public List<String> get(final String tableName, final byte[] rowName, final String familyName,
			final String qualifier) {
		return htemplate.execute(tableName, new TableCallback<List<String>>() {

			@Override
			public List<String> doInTable(HTableInterface table) throws Throwable {
				Get get = new Get(rowName);
				get.setMaxVersions(3); // 设置一次性获取多少个版本的数据
				get.addColumn(familyName.getBytes(), qualifier.getBytes());
				Result result = table.get(get);
				List<Cell> cells = result.listCells();
				String res = "";
				List<String> list = new ArrayList<String>();
				if (null != cells && !cells.isEmpty()) {
					for (Cell ce : cells) {
						res = Bytes.toString(ce.getValueArray(), ce.getValueOffset(), ce.getValueLength());
						System.out.println("res:" + res + " timestamp:" + ce.getTimestamp());
						list.add(res);
					}
				}
				return list;
			}
		});
	}
}
