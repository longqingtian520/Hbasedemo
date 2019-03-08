package com.criss.wang.controller;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.criss.wang.entity.Employee;
import com.criss.wang.service.HbaseService;

@RestController
public class HbaseController {

	@Autowired
	private HbaseService hService;

	/**
	 * 判断表是否存在
	 *
	 * @return
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	@RequestMapping(value = "/exist", method = RequestMethod.GET)
	public String getExistData() throws ZooKeeperConnectionException, IOException {
		if (hService.isExist("test_crisstb1")) {
			return "存在";
		} else {
			return "不存在";
		}
	}

	/**
	 * 查询表中的总行数
	 *
	 * @param tableName
	 * @return
	 */
	@GetMapping(path = "/row/count/{tableName}")
	public String getRowNums(@PathVariable(value = "tableName") String tableName) {
		try {
			if (hService.isExist(tableName)) {
				return hService.getRowNumsWithTable(tableName);
			}
			return "表不存在";
		}catch(Exception e) {
			e.printStackTrace();
			return "程序异常";
		}

	}

	/**
	 * 获取指定单元格的值
	 *
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param column
	 * @return
	 */
	@RequestMapping(value = "/cell/data/{tableName}/{rowKey}/{familyColumn}/{column}", method = RequestMethod.GET)
	public String getCellData(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "rowKey", required = false) String rowKey,
			@PathVariable(value = "familyColumn", required = false) String familyColumn,
			@PathVariable(value = "column", required = false) String column) {
		try {
			if (hService.isExist(tableName)) {
				return hService.getCellData(tableName, rowKey, familyColumn, column);
			} else {
				return "表不存在";
			}
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * 向hbase插入单行数据
	 *
	 * @param tableName
	 * @param rowKey
	 * @param familyColumn
	 * @param column
	 * @param value
	 * @return
	 */
	@RequestMapping(value = "/cell/data/{tableName}/{rowKey}/{familyColumn}/{column}/{value}", method = RequestMethod.POST)
	public String addCellData(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "rowKey", required = false) String rowKey,
			@PathVariable(value = "familyColumn", required = false) String familyColumn,
			@PathVariable(value = "column", required = false) String column,
			@PathVariable(value = "value", required = false) String value) {
		try {
			if (hService.isExist(tableName)) {
				return hService.addCellData(tableName, rowKey, familyColumn, column, value);
			} else {
				return "表不存在";
			}
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * 向hbase插入多行数据
	 *
	 * @param tableName
	 *            表名称
	 * @param flag
	 *            插入方式标识
	 * @param emps
	 *            员工信息
	 * @return
	 */
	@RequestMapping(value = "/row/data/{tableName}", method = RequestMethod.POST)
	public String insertData(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "flag", required = true) int flag, @RequestBody List<Employee> emps) {
		try {
			if (hService.isExist(tableName)) {
				return hService.insertData(tableName, emps, flag);
			} else {
				return "表不存在";
			}
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * 查询数据
	 *
	 * @param tableName
	 *            表名称
	 * @param rowKey
	 *            行键
	 * @return
	 */
	@RequestMapping(value = "/row", method = RequestMethod.GET)
	public String getDataFromHBase(@RequestParam(value = "tableName", required = false) String tableName,
			@RequestParam(value = "rowKey", required = false) String rowKey) {
		try {
			hService.getDataFromHBase(tableName, rowKey);
			return "success";
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * 批量添加数据
	 */
	@RequestMapping(value = "/batch/{tableName}", method = RequestMethod.POST)
	public String batchAdd(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestBody List<Employee> emps) {
		try {
			return hService.batchAdd(tableName, emps);
		} catch (Exception e) {
			return "程序异常";
		}

	}

	/**
	 * 批量删除
	 *
	 * @param tableName
	 * @param rowKeys
	 * @return
	 */
	@RequestMapping(value = "/batch/{tableName}", method = RequestMethod.DELETE)
	public String batchDelete(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestBody List<String> rowKeys) {
		try {
			return hService.batchDelete(tableName, rowKeys);
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * 批量获取数据
	 *
	 * @param tableName
	 * @param rowKeys
	 * @return
	 */
	@RequestMapping(value = "/batch/{tableName}", method = RequestMethod.GET)
	public String batchGet(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestBody List<String> rowKeys) {
		try {
			return hService.batchGet(tableName, rowKeys);
		} catch (Exception e) {
			return "程序异常";
		}
	}

	/**
	 * checkAndPut语法
	 *
	 * @param tableName
	 * @param rowKey
	 * @param standard
	 * @param emp
	 * @return
	 */
	@RequestMapping(value = "/check/put/{tableName}", method = RequestMethod.POST)
	public String checkAndPut(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestParam(value = "rowKey", required = false) String rowKey,
			@RequestParam(value = "standard", required = false) String standard, @RequestBody Employee emp) {
		try {
			return hService.checkAndPut(tableName, emp, rowKey, standard);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 扫描数据
	 *
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @return
	 */
	@RequestMapping(value = "/scanner", method = RequestMethod.GET)
	public String scanData(@RequestParam(value = "tableName", required = false) String tableName,
			@RequestParam(value = "startRow", required = false) String startRow,
			@RequestParam(value = "endRow", required = false) String endRow) {
		try {
			return hService.scanData(tableName, startRow, endRow);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * Mutation语法
	 *
	 * @param tableName
	 * @param rowKey
	 * @param newAge
	 * @param delName
	 * @param modName
	 * @return
	 */
	@RequestMapping(value = "/mutation/{tableName}", method = RequestMethod.POST)
	public String mutatioin(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestParam(value = "rowKey", required = false) String rowKey,
			@RequestParam(value = "newAge", required = false) String newAge,
			@RequestParam(value = "delName", required = false) String delName,
			@RequestParam(value = "modName", required = false) String modName) {
		try {
			return hService.mutationData(tableName, rowKey, newAge, delName, modName);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 值过滤器
	 *
	 * @param tableName
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/value/filter/{tableName}/{name}", method = RequestMethod.GET)
	public String valueFilter(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "name", required = false) String name) {
		try {
			return hService.valueFilter(tableName, name);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 单列值过滤器
	 *
	 * @param tableName
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/single/value/filter/{tableName}/{name}", method = RequestMethod.GET)
	public String singleCloumnValueFilter(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "name", required = false) String name) {
		try {
			return hService.singleColumnValueFilter(tableName, name);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 过滤器列表
	 *
	 * @param tableName
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/filter/list/{tableName}/{name}/{isAccurate}", method = RequestMethod.GET)
	public String fliterList(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "name", required = false) String name,
			@PathVariable(value = "isAccurate", required = false) boolean isAccurate) {
		try {
			return hService.fliterList(tableName, name, isAccurate);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 过滤数字
	 *
	 * @param tableName
	 * @param value
	 * @return
	 */
	@RequestMapping(value = "/num/filter/{tableName}/{value}", method = RequestMethod.GET)
	public String numberFilter(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "value", required = false) int value) {
		try {
			return hService.numberFilter(tableName, value);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 分页过滤
	 *
	 * @param tableName
	 * @param value
	 * @return
	 */
	@RequestMapping(value = "/page/filter/{tableName}/{value}", method = RequestMethod.GET)
	public String pageFilter(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "value", required = false) long value) {
		try {
			return hService.pageFilter(tableName, value);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 连续分页
	 *
	 * @param tableName
	 * @param value
	 * @return
	 */
	@RequestMapping(value = "/sequence/page/filter/{tableName}/{value}", method = RequestMethod.GET)
	public String sequencePageFilter(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "value", required = false) long value) {
		try {
			return hService.sequencePageFilter(tableName, value);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 过滤器顺序
	 *
	 * @param tableName
	 * @param value
	 * @param page
	 * @return
	 */
	@RequestMapping(value = "/order/filter/list/{tableName}/{value}/{page}", method = RequestMethod.GET)
	public String listFilterOrder(@PathVariable(value = "tableName", required = false) String tableName,
			@PathVariable(value = "value", required = false) String value,
			@PathVariable(value = "page", required = false) long page) {
		try {
			return hService.listFilterOrder(tableName, value, page);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * And OR 过滤
	 *
	 * @param tableName
	 * @param oneCity
	 * @param twoCity
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/logic/list/filter/{tableName}")
	public String listFilterAndOr(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestParam(value = "oneCity", required = false) String oneCity,
			@RequestParam(value = "twoCity", required = false) String twoCity,
			@RequestParam(value = "name", required = false) String name) {
		try {
			return hService.listFilterAndOr(tableName, oneCity, twoCity, name);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 行键过滤器
	 *
	 * @param rowKey
	 * @return
	 */
	@RequestMapping(value = "/row/filter/{rowKey}", method = RequestMethod.GET)
	public String rowFilter(@PathVariable(value = "rowKey", required = false) String rowKey) {
		try {
			return hService.rowFilter(rowKey);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 多行过滤器
	 *
	 * @return
	 */
	@RequestMapping(value = "/mutil/row/range", method = RequestMethod.GET)
	public String mutilRowRangeFilter() {
		try {
			return hService.rowRangeFilter();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 前缀过滤器
	 *
	 * @param prefix
	 * @return
	 */
	@RequestMapping(value = "/prefix/{prefix}", method = RequestMethod.GET)
	public String prefixFilter(@PathVariable(value = "prefix", required = false) String prefix) {
		try {
			return hService.prefixRowFilter(prefix);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 行键模糊匹配
	 *
	 * @param fuzzy
	 * @return
	 */
	@RequestMapping(value = "/fuzzy", method = RequestMethod.GET)
	public String fuzzyFilter(@RequestParam(value = "rowKey", required = false) String rowKey) {
		try {
			return hService.fuzzyRowFilter(rowKey);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 包含结尾过滤器
	 *
	 * @return
	 */
	@RequestMapping(value = "/inclusive/stop", method = RequestMethod.GET)
	public String inclusiveStopFilter() {
		try {
			return hService.inclusiveStopFilter();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 随机过滤器
	 *
	 * @return
	 */
	@RequestMapping(value = "/random/{chance}", method = RequestMethod.GET)
	public String randomRowFilter(@PathVariable(value = "chance", required = false) float chance) {
		try {
			return hService.randomRowFilter(chance);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 生成新的Hbase数据
	 *
	 * @return
	 */
	@RequestMapping(value = "/generate/data", method = RequestMethod.POST)
	public String generateHbaseData() {
		try {
			return hService.generateHbaseData();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 依赖列过滤器
	 *
	 * @return
	 */
	@RequestMapping(value = "/dependent/filter", method = RequestMethod.GET)
	public String dependentColumnFilter() {
		try {
			return hService.dependentColumnFilter();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 列前缀过滤器
	 *
	 * @param prefix
	 * @return
	 */
	@RequestMapping(value = "/prefix/column/{prefix}", method = RequestMethod.GET)
	public String prefixColumnFilter(@PathVariable(value = "prefix", required = false) String prefix) {
		try {
			return hService.prefixColumnFilter(prefix);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 多列前缀过滤器
	 *
	 * @param prex1
	 * @param prex2
	 * @return
	 */
	@RequestMapping(value = "/mutil/prefix/column", method = RequestMethod.GET)
	public String multiColumnPrefixFilter(@RequestParam(value = "prefix1", required = false) String prefix1,
			@RequestParam(value = "prefix2", required = false) String prefix2) {
		try {
			return hService.multiColumnPrefixFilter(prefix1, prefix2);
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 列名过滤器
	 */
	@RequestMapping(value = "/key/column/filter", method = RequestMethod.GET)
	public String KeyOnlyFilter() {
		try {
			return hService.KeyOnlyFilter();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 首次列键过滤器
	 *
	 * @return
	 */
	@RequestMapping(value = "/count", method = RequestMethod.GET)
	public String firstKeyOnlyFilter() {
		try {
			return hService.firstKeyOnlyFilter();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 获取Region列表
	 *
	 * @return
	 */
	@RequestMapping(value = "/region/list", method = RequestMethod.GET)
	public String getRegions() {
		try {
			return hService.getRegions();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 创建快照表
	 *
	 * @return
	 */
	@RequestMapping(value = "/create/snapshot", method = RequestMethod.POST)
	public String createSnapshot() {
		try {
			return hService.createSnapshot();
		} catch (IOException e) {
			return "程序异常";
		}
	}

	/**
	 * 操作快照
	 *
	 * @param tableName
	 * @return
	 */
	@RequestMapping(value = "/snapshot", method = RequestMethod.GET)
	public String operateSnapshot(@RequestParam(value = "tableName", required = false) String tableName) {
		try {
			return hService.operateSnapshot(tableName);
		} catch (IOException e) {
			return "程序异常";
		}
	}

}
