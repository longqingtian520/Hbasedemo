package com.criss.wang.controller;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
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
		if (hService.isExist("test_crisstb")) {
			return "存在";
		} else {
			return "不存在";
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

}
