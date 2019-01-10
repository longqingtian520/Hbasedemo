package com.criss.wang.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

/**
 *
 * author: wangqiubao
 *
 * date: 2019-01-10 10:54:45
 *
 * describe:
 */
@Service
public class HbaseService {

	private static final Logger logger = LoggerFactory.getLogger(HbaseService.class);

	@Autowired
	private HbaseTemplate htemplate;

	/**
	 * 初始化HBASE链接
	 *
	 * @throws IOException
	 */
	public Connection initHbase() throws IOException {
		Configuration config = htemplate.getConfiguration();
		return ConnectionFactory.createConnection(config);
	}

	/**
	 * 判断表是否存在
	 *
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean isExist(String tableName) throws IOException {
		Connection conn = initHbase();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		return admin.tableExists(tableName);
	}

	/**
	 * 查询指定单元格的值
	 *
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param column
	 * @return
	 * @throws IOException
	 */
	public String getCellData(String tableName, String rowKey, String familyColumn, String column) throws IOException {
		Table table = initHbase().getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		if (!get.isCheckExistenceOnly()) {
			get.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(column));
			Result result = table.get(get);
			byte[] resultBytes = result.getValue(Bytes.toBytes(familyColumn), Bytes.toBytes(column));
			return Bytes.toString(resultBytes);
		} else {
			return "No value";
		}
	}

}
