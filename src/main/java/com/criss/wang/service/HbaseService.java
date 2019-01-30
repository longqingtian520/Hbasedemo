package com.criss.wang.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
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

	private static final String SUCCESS = "success";

	private static final String FAILURE = "failure";

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
	 * 删除表
	 *
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public String deleteTable(String tableName) throws Exception {
		if (isExist(tableName)) {
			Connection conn = initHbase();
			HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(tableName);
		}
		return SUCCESS;
	}

	/**
	 * 创建或覆盖表
	 *
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public String createOrOverwriteTable(HTableDescriptor table) throws Exception {
		Connection conn = initHbase();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
			admin.createTable(table);
			return SUCCESS;
		}
		admin.createTable(table);
		return SUCCESS;
	}

	/**
	 * 添加新的列族
	 *
	 * @param tableName
	 *            表名称
	 * @param cf
	 *            新列族
	 * @return
	 * @throws IOException
	 */
	public String addNewColumnFamily(String tableName, String cf) throws IOException {
		Connection conn = initHbase();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		HColumnDescriptor column = new HColumnDescriptor(cf);
		column.setCompactionCompressionType(Algorithm.GZ);
		column.setMaxVersions(Integer.MAX_VALUE);
		admin.addColumn(tableName, column);
		return SUCCESS;
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
	 * @param flag
	 * @return
	 * @throws IOException
	 */
	public String insertData(String tableName, List<Employee> emps, int flag) throws IOException {
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

	/**
	 * 查询Hbase值
	 *
	 * @param tableName
	 *            表名称
	 * @param rowKey
	 *            键值
	 */
	public void getDataFromHBase(String tableName, String rowKey) throws Exception {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		get.setMaxVersions(5);
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("键值：" + Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("列值：" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
		System.out.println("============================================================");
		// List<Cell> list = result.getColumnCells(Bytes.toBytes("personal"),
		// Bytes.toBytes("name"));
		// for(Cell cell : list) {
		// System.out.println("键值：" + Bytes.toString(CellUtil.cloneRow(cell)));
		// System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
		// System.out.println("列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
		// System.out.println("列值：" + Bytes.toString(CellUtil.cloneValue(cell)));
		// }

	}

	/**
	 * 批量插入
	 *
	 * @param tableName
	 * @param emps
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String batchAdd(String tableName, List<Employee> emps) throws IOException, InterruptedException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		List<Put> puts = new ArrayList<>();
		Put put = null;
		for (Employee emp : emps) {
			put = new Put(Bytes.toBytes(new Date().getTime() + ""));
			put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes(emp.getCity()));
			put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(emp.getName()));

			put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), Bytes.toBytes(emp.getManager()));
			put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes(emp.getSalary()));
			puts.add(put);
			Thread.sleep(1000);
		}
		Object[] object = new Object[puts.size()];
		table.batch(puts, object);
		return "success";
	}

	/**
	 * 批量删除
	 *
	 * @param tableName
	 * @param rowKeys
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String batchDelete(String tableName, List<String> rowKeys) throws IOException, InterruptedException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		List<Delete> deletes = new ArrayList<>();
		Delete delete = null;
		for (String rowKey : rowKeys) {
			delete = new Delete(Bytes.toBytes(rowKey));
			deletes.add(delete);
		}
		Object[] object = new Object[deletes.size()];
		table.batch(deletes, object);
		return "success";
	}

	/**
	 * 批量获取数据(当且仅当只有一个版本时)
	 * @param tableName
	 * @param rowKeys
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String batchGet(String tableName, List<String> rowKeys) throws IOException, InterruptedException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		List<Employee> emps = new ArrayList<>();
		Employee  employee = null;
		List<Get> gets = new ArrayList<>();
		Get get = null;
		for(String rowKey: rowKeys) {
			get = new Get(Bytes.toBytes(rowKey));
			gets.add(get);
		}
		Result[] results = table.get(gets);
		for(int i= 0; i< results.length; i++) {
			employee = new Employee();
			Result result = results[i];
			List<Cell> cells = result.getColumnCells(Bytes.toBytes("personal"), Bytes.toBytes("city"));
			for(Cell cell : cells) {
				employee.setCity(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells1 = result.getColumnCells(Bytes.toBytes("personal"), Bytes.toBytes("name"));
			for(Cell cell : cells1) {
				employee.setName(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells2 = result.getColumnCells(Bytes.toBytes("professional"), Bytes.toBytes("manager"));
			for(Cell cell : cells2) {
				employee.setManager(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells3 = result.getColumnCells(Bytes.toBytes("professional"), Bytes.toBytes("salary"));
			for(Cell cell : cells3) {
				employee.setSalary(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			emps.add(employee);
		}
		return objectMapper.writeValueAsString(emps);
	}

}
