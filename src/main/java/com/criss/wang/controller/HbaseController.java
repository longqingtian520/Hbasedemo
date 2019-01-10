package com.criss.wang.controller;

import java.io.IOException;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.criss.wang.service.HbaseService;

@RestController
public class HbaseController {

	@Autowired
	private HbaseService hService;

	@RequestMapping(value = "/exist", method = RequestMethod.GET)
	public String getExistData() throws ZooKeeperConnectionException, IOException {
		if(hService.isExist("test_crisstb")) {
			return "存在";
		}else {
			return "不存在";
		}
	}
}
