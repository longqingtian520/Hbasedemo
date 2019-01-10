package com.criss.wang.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
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
	public boolean isExist(String tableName) throws IOException {
		Configuration conf = htemplate.getConfiguration();
		Connection conn = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		return admin.tableExists(tableName);
	}




}
