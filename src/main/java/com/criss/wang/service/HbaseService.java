package com.criss.wang.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

	private static final String TABLE_NAME = "test_crisstb";

	private static final String SUCCESS = "success";

	// private static final String FAILURE = "failure";

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
	 * 查询表中的数据量
	 *
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public String getRowNumsWithTable(String tableName) throws IOException {

		long start = System.currentTimeMillis();

		Connection conn = initHbase();
		Table table = conn.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		scan.setFilter(filter);

		ResultScanner result = table.getScanner(scan);
		int count = 0;
		for (Result r : result) {
			count++;
		}

		result.close();

		long end = System.currentTimeMillis();

		System.out.println("表，一共有 " + count + " 条数据， 总耗时：" + (end - start) / 1000);

		return count + "";
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
		// put.setWriteToWAL(false); // 关闭写入Hlog ，该方法已被弃用，如下面那行代码
		// put.setDurability(Durability.SKIP_WAL); // 关闭写入Hlog
		put.setDurability(Durability.ASYNC_WAL); // 异步写入HLog， Hlog是WAL的实现类
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
		return SUCCESS;
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
		return SUCCESS;
	}

	/**
	 * 批量获取数据(当且仅当只有一个版本时)
	 *
	 * @param tableName
	 * @param rowKeys
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String batchGet(String tableName, List<String> rowKeys) throws IOException, InterruptedException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		List<Employee> emps = new ArrayList<>();
		Employee employee = null;
		List<Get> gets = new ArrayList<>();
		Get get = null;
		for (String rowKey : rowKeys) {
			get = new Get(Bytes.toBytes(rowKey));
			gets.add(get);
		}
		Result[] results = table.get(gets);
		for (int i = 0; i < results.length; i++) {
			employee = new Employee();
			Result result = results[i];
			List<Cell> cells = result.getColumnCells(Bytes.toBytes("personal"), Bytes.toBytes("city"));
			for (Cell cell : cells) {
				employee.setCity(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells1 = result.getColumnCells(Bytes.toBytes("personal"), Bytes.toBytes("name"));
			for (Cell cell : cells1) {
				employee.setName(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells2 = result.getColumnCells(Bytes.toBytes("professional"), Bytes.toBytes("manager"));
			for (Cell cell : cells2) {
				employee.setManager(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			List<Cell> cells3 = result.getColumnCells(Bytes.toBytes("professional"), Bytes.toBytes("salary"));
			for (Cell cell : cells3) {
				employee.setSalary(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			emps.add(employee);
		}
		return objectMapper.writeValueAsString(emps);
	}

	/**
	 * checkAndPut语法
	 *
	 * @param tableName
	 * @param emp
	 * @param rowKey
	 * @param standard
	 * @return
	 * @throws IOException
	 */
	public String checkAndPut(String tableName, Employee emp, String rowKey, String standard) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(emp.getName()));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes(emp.getCity()));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), Bytes.toBytes(emp.getManager()));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes(emp.getSalary()));

		table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("personal"), Bytes.toBytes("name"), CompareOp.EQUAL,
				Bytes.toBytes(standard), put);

		return SUCCESS;
	}

	/**
	 * scanner语法
	 *
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @return
	 * @throws IOException
	 */
	public String scanData(String tableName, String startRow, String endRow) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.setBatch(1); // 一次扫描数据
		scan.setCaching(1000); // hbase扫描时的缓存
		if (!StringUtils.isEmpty(startRow)) {
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		if (!StringUtils.isEmpty(endRow)) {
			scan.setStopRow(Bytes.toBytes(endRow));
		}
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println(Bytes.toString(result.getRow()));
		}

		return SUCCESS;
	}

	/**
	 * Mutation语法
	 *
	 * @param tableName
	 * @param rowKey
	 * @param delName
	 * @param modName
	 * @param newValue
	 * @return
	 * @throws IOException
	 */
	public String mutationData(String tableName, String rowKey, String newValue, String delName, String modName)
			throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));

		// 删除
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes("personal"), Bytes.toBytes(delName));

		// 新增
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("age"), Bytes.toBytes(newValue));

		// 修改
		Put edit = new Put(Bytes.toBytes(rowKey));
		edit.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(modName));

		RowMutations rowMutation = new RowMutations(Bytes.toBytes(rowKey));
		rowMutation.add(delete);
		rowMutation.add(put);
		rowMutation.add(edit);

		table.mutateRow(rowMutation);
		return SUCCESS;
	}

	/**
	 * 值过滤器 等同于LIKE
	 *
	 * @param tableName
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public String valueFilter(String tableName, String value) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("wang"));
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);

		for (Result result : rs) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			System.out.println(name);
		}
		rs.close();
		return SUCCESS;
	}

	/**
	 * 单列值过滤器
	 *
	 * @param tableName
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public String singleColumnValueFilter(String tableName, String value) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("personnal"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator("wang"));
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);

		for (Result result : rs) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			System.out.println(name);
		}
		rs.close();
		return SUCCESS;
	}

	/**
	 * 过滤器列表
	 */
	public String fliterList(String tableName, String value, boolean isAccurate) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		// 创建过滤器列表
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

		// 只有列族为personal的记录才放入结果集
		Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("personal")));
		filterList.addFilter(familyFilter);

		// 只有列为name的记录才放入结果集中
		Filter columnFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("name")));
		filterList.addFilter(columnFilter);

		// 只有值包含wang的记录才被放入结果集中
		if (isAccurate) {
			Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(value)));
			filterList.addFilter(valueFilter);
		} else {
			Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
			filterList.addFilter(valueFilter);
		}

		scan.setFilter(filterList);
		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			System.out.println(name);
		}
		rs.close();
		return SUCCESS;
	}

	/**
	 * 数字过滤 有问题 TODO
	 *
	 * @param tableName
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public String numberFilter(String tableName, int value) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("professional"), Bytes.toBytes("salary"),
				CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(value)));
		scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			String num = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("salary")));
			System.out.println(name + " : " + num);
		}
		rs.close();
		return SUCCESS;
	}

	/**
	 * 分页
	 *
	 * @param tableName
	 * @param page
	 * @return
	 * @throws IOException
	 */
	public String pageFilter(String tableName, long page) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter pageFilger = new PageFilter(page);
		scan.setFilter(pageFilger);
		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			System.out.println(name);
		}
		return SUCCESS;
	}

	/**
	 * 连续分页打印
	 *
	 * @param tableName
	 * @param page
	 * @return
	 * @throws IOException
	 */
	public String sequencePageFilter(String tableName, long page) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		Filter pageFilter = new PageFilter(page);
		scan.setFilter(pageFilter);
		// 第一页
		System.out.println("第一页数据");
		ResultScanner rs = table.getScanner(scan);
		byte[] rowKey = printResult(rs);
		rs.close();

		// 打印第二页
		System.out.println("第二页数据");
		byte[] startRowKey = Bytes.add(rowKey, new byte[1]);
		scan.setStartRow(startRowKey);
		rs = table.getScanner(scan);
		printResult(rs);
		rs.close();

		return SUCCESS;
	}

	private byte[] printResult(ResultScanner rs) {
		byte[] lastRowKey = null;
		for (Result result : rs) {
			byte[] rowKey = result.getRow();
			String row = Bytes.toString(rowKey);
			String city = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city")));
			String name = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name")));
			String manager = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("manager")));
			System.out.println(row + " : " + city + " : " + name + " : " + manager);
			lastRowKey = rowKey;
		}
		return lastRowKey;
	}

	/**
	 * 探究过滤器顺序对结果集的影响 ==== 是因为各个过滤器在过滤器列表内的执行是有先后顺序
	 *
	 * @param talbeName
	 * @param page
	 * @return
	 * @throws IOException
	 */
	public String listFilterOrder(String tableName, String value, long page) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		List<Filter> filters = new ArrayList<>();

		// 分页过滤
		Filter pageFilter = new PageFilter(page);
		filters.add(pageFilter);

		// 单值过滤
		Filter valueFilter = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
		filters.add(valueFilter);

		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);

		ResultScanner rs = table.getScanner(scan);
		printResult(rs);

		return SUCCESS;
	}

	/**
	 * And OR过滤
	 *
	 * @param tableName
	 * @param OneCity
	 * @param TwoCity
	 * @param name
	 * @return
	 * @throws IOException
	 */
	public String listFilterAndOr(String tableName, String OneCity, String TwoCity, String name) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();

		FilterList innerFilter = new FilterList(Operator.MUST_PASS_ONE);

		Filter qdFilter = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("city"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(OneCity));
		innerFilter.addFilter(qdFilter);

		Filter bjFilter = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("city"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(TwoCity));
		innerFilter.addFilter(bjFilter);

		FilterList outerFilter = new FilterList(Operator.MUST_PASS_ALL);

		outerFilter.addFilter(innerFilter);
		Filter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(name));
		outerFilter.addFilter(nameFilter);

		scan.setFilter(outerFilter);

		ResultScanner rs = table.getScanner(scan);

		printResult(rs);

		return SUCCESS;
	}

	/**
	 * 行键过滤器
	 *
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public String rowFilter(String rowKey) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(rowKey)));

		scan.setFilter(rowFilter);
		ResultScanner rs = table.getScanner(scan);
		printResult(rs);
		return SUCCESS;
	}

	/**
	 * MutilRowRange多行范围过滤器
	 *
	 * @return
	 * @throws IOException
	 */
	public String rowRangeFilter() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();

		List<RowRange> list = new ArrayList<>();

		RowRange rowRang1 = new RowRange("1548825906109", true, "1549070899150", true);
		RowRange rowRange2 = new RowRange("1549071063328", true, "1549071065329", true);
		list.add(rowRang1);
		list.add(rowRange2);
		Filter filter = new MultiRowRangeFilter(list);

		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);

		printResult(rs);
		return SUCCESS;
	}

	/**
	 * 行键前缀过滤器
	 *
	 * @param prefix
	 * @return
	 * @throws IOException
	 */
	public String prefixRowFilter(String prefix) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
		scan.setFilter(prefixFilter);
		ResultScanner rs = table.getScanner(scan);

		printResult(rs);
		return SUCCESS;
	}

	/**
	 * 行键模糊匹配
	 *
	 * @param fuzzyRow
	 * @return
	 * @throws IOException
	 */
	public String fuzzyRowFilter(String fuzzyRow) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		List<Pair<byte[], byte[]>> list = new ArrayList<>();
		Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(Bytes.toBytesBinary(fuzzyRow),
				new byte[] { 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0 });
		list.add(pair);
		FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(list);

		Filter fuzzy = new FuzzyRowFilter(Arrays.asList(new Pair<byte[], byte[]>(Bytes.toBytesBinary(fuzzyRow),
				new byte[] { 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0 })));

		scan.setFilter(fuzzyFilter);
		ResultScanner rs = table.getScanner(scan);
		printResult(rs);
		return SUCCESS;
	}

	/**
	 * 包含结尾过滤器
	 *
	 * @return
	 * @throws IOException
	 */
	public String inclusiveStopFilter() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan(Bytes.toBytes("1548825906109"));
		Filter filter = new InclusiveStopFilter(Bytes.toBytes("1549070900152"));
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		printResult(rs);
		return SUCCESS;
	}

	/**
	 * 随机行过滤器
	 *
	 * @param chance
	 * @return
	 * @throws IOException
	 */
	public String randomRowFilter(float chance) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		Filter filter = new RandomRowFilter(new Float(chance));
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		printResult(rs);
		return SUCCESS;
	}

	// 列过滤器

	/**
	 * 新造一批数据，指定时间戳
	 *
	 * @return
	 * @throws IOException
	 */
	public String generateHbaseData() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));

		List<Put> puts = new ArrayList<>();

		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), 1L, Bytes.toBytes("南京"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), 1L, Bytes.toBytes("xiaoming"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), 1L, Bytes.toBytes("wuli"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), 1L, Bytes.toBytes(10000));
		puts.add(put);

		put = new Put(Bytes.toBytes("row2"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), 1L, Bytes.toBytes("nanjing"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), 1L, Bytes.toBytes("songjiang"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), 1L, Bytes.toBytes("zhengzhi"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), 1L, Bytes.toBytes(12000));
		puts.add(put);

		put = new Put(Bytes.toBytes("row3"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), 1L, Bytes.toBytes("xining"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), 1L, Bytes.toBytes("huakui"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), 1L, Bytes.toBytes("wushu"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), 1L, Bytes.toBytes(12500));
		puts.add(put);

		put = new Put(Bytes.toBytes("row4"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), 1L, Bytes.toBytes("changsha"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), 1L, Bytes.toBytes("luzhishen"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("manager"), 1L, Bytes.toBytes("shaolinsi"));
		put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), 1L, Bytes.toBytes(12000));
		puts.add(put);

		table.put(puts);

		return SUCCESS;
	}

	/**
	 * 依赖列过滤器
	 *
	 * @return
	 * @throws IOException
	 */
	public String dependentColumnFilter() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		Filter filter = new DependentColumnFilter(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		printResult(rs);

		return SUCCESS;
	}

	/**
	 * 列前缀过滤器
	 *
	 * @param prefix
	 * @return
	 * @throws IOException
	 */
	public String prefixColumnFilter(String prefix) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);

		printResult(rs);
		return SUCCESS;
	}

	/**
	 * 多列前缀过滤器
	 *
	 * @param prex1
	 * @param prex2
	 * @return
	 * @throws IOException
	 */
	public String multiColumnPrefixFilter(String prex1, String prex2) throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		byte[][] prefix = new byte[2][];
		prefix[0] = Bytes.toBytes(prex1);
		prefix[1] = Bytes.toBytes(prex2);
		MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefix);
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		printResult(rs);

		return SUCCESS;
	}

	/**
	 * 列名过滤器
	 *
	 * @return
	 * @throws IOException
	 */
	public String KeyOnlyFilter() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();

		KeyOnlyFilter filter = new KeyOnlyFilter();
		scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			String rowKey = Bytes.toString(result.getRow());
			List<Cell> cells = result.listCells();
			List<String> columnNames = new ArrayList<>();
			columnNames.add(rowKey);
			for (Cell cell : cells) {
				columnNames.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
			}
			System.out.println(objectMapper.writeValueAsString(columnNames));
			columnNames.clear();
		}
		return SUCCESS;
	}

	/**
	 * 首次列过滤器+
	 *
	 * @return
	 * @throws IOException
	 */
	public String firstKeyOnlyFilter() throws IOException {
		HTable table = (HTable) initHbase().getTable(TableName.valueOf("uav_fly_data"));

		long begintime = System.currentTimeMillis();

		Scan scan = new Scan();
		FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);

		int count = 1;
		for (Result result : rs) {
			count++;
		}

		long endtime = System.currentTimeMillis();
		long haoshi = (endtime - begintime) / 1000l;
		System.out.println("总共有：" + count + " 行数据， 耗时：" + haoshi + " 秒");
		return SUCCESS;
	}

	/**
	 * 获取RegionServer 和 对应的region列表
	 *
	 * @return
	 * @throws IOException
	 */
	public String getRegions() throws IOException {
		Admin admin = initHbase().getAdmin();
		Collection<ServerName> serverNames = admin.getClusterStatus().getServers();
		Iterator<ServerName> iterator = serverNames.iterator();
		while (iterator.hasNext()) {
			ServerName serverName = iterator.next();
			System.out.println("\n RegionServer ：" + serverName.getServerName() + ", 拥有以下Region：");

			List<HRegionInfo> regions = admin.getOnlineRegions(serverName);
			for (HRegionInfo region : regions) {
				System.out.println(region.getRegionNameAsString());
			}
		}

		System.out.println("=========================================================");

		String master = admin.getClusterStatus().getMaster().getServerName();
		String ip = admin.getClusterStatus().getMaster().getHostAndPort();
		System.out.println("master: " + master + ", ip : " + ip);

		return SUCCESS;
	}

	/**
	 * 生成快照
	 *
	 * @return
	 * @throws IOException
	 */
	public String createSnapshot() throws IOException {
		Admin admin = initHbase().getAdmin();
		admin.snapshot("test_snapshot", TableName.valueOf(TABLE_NAME));
		return SUCCESS;
	}

	/**
	 * 操作快照
	 *
	 * @param snapeshot
	 * @return
	 * @throws IOException
	 */
	public String operateSnapshot(String snapeshot) throws IOException {
		Admin admin = initHbase().getAdmin();
		// 列出所有快照
		List<SnapshotDescription> snapshots = admin.listSnapshots();
		System.out.println("快照数量：" + snapshots.size());
		for (SnapshotDescription sd : snapshots) {
			System.out.println(sd.getName());
		}

		// 快照回复数据 步骤：1、先禁用数据表；2、快照恢复；3、启用数据表
		admin.disableTable(TableName.valueOf(TABLE_NAME));
		admin.restoreSnapshot("test_snapshot");
		admin.enableTable(TableName.valueOf(TABLE_NAME));

		System.out.println("成功");
		return SUCCESS;
	}
}
