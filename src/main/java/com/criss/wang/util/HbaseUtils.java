package com.criss.wang.util;

import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtils {

	public static byte[] asRowKey(Object... vals) {
		return Bytes.toBytes(asRowKeyStr(vals));
	}

	public static String asRowKeyStr(Object... vals) {

		StringBuilder b = new StringBuilder();
		for (int i = 0; i < vals.length; ++i) {
			b.append(vals[i]);
			if ((i + 1) < vals.length)
				b.append(":");
		}
		return b.toString();
	}

	public static String asColNameStr(Object... ele) {
		return asRowKeyStr(ele);
	}

	public static byte[] asColName(Object... ele) {
		return asRowKey(ele);
	}
}
