package com.criss.wang.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.criss.wang.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;

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

	@Autowired
	private ObjectMapper objectMapper;

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

	/**
	 * 向表中添加数据
	 *
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param column
	 * @param value
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public String addCellData(String tableName, String rowKey, String familyColumn, String column, String value)
			throws IllegalArgumentException, IOException {
		// 创建Hbase表对象
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		logger.info("insert data to hbase successed");
		return "insert data to hbase successed";
	}

	/**
	 * 多行插入
	 *
	 * @param tableName
	 * @param emps
	 * @return
	 * @throws IOException
	 */
	public String insertData(String tableName, List<Employee> emps) throws IOException {
		// 获取所有字段
		Field[] fields = Employee.class.getDeclaredFields();
		logger.info(objectMapper.writeValueAsString(fields));
		// 常见HBASE表对象
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		for (Employee emp : emps) {
			Put put = new Put(Bytes.toBytes(new Date().getTime() + ""));
			if (!StringUtils.isEmpty(emp.getName())) {
				put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes(fields[0].getName()),
						Bytes.toBytes(emp.getName()));
			}
			if (!StringUtils.isEmpty(emp.getCity())) {
				put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes(fields[1].getName()),
						Bytes.toBytes(emp.getCity()));
			}
			if (!StringUtils.isEmpty(emp.getManager())) {
				put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes(fields[2].getName()),
						Bytes.toBytes(emp.getManager()));
			}
			if (!StringUtils.isEmpty(emp.getSalary())) {
				put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes(fields[3].getName()),
						Bytes.toBytes(emp.getSalary()));
			}
			table.put(put);
		}
		return "insert data successed";
	}

}
