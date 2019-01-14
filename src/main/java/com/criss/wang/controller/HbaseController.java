package com.criss.wang.controller;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
	 * @param emps
	 * @return
	 */
	@RequestMapping(value = "/row/data/{tableName}", method = RequestMethod.POST)
	public String insertData(@PathVariable(value = "tableName", required = false) String tableName,
			@RequestBody List<Employee> emps) {
		try {
			if (hService.isExist(tableName)) {
				return hService.insertData(tableName, emps);
			} else {
				return "表不存在";
			}
		} catch (Exception e) {
			return "程序异常";
		}
	}
}
