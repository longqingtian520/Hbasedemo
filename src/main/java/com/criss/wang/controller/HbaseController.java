package com.criss.wang.controller;

import java.io.IOException;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
}
