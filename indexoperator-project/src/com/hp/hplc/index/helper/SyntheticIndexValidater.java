package com.hp.hplc.index.helper;

import java.io.FileReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.hp.hplc.index.CassandraPartitionedIndexAccessor;
import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.index.helper.CassandraIndexMeta;
import com.hp.hplc.util.Pair;

/**
 * Synthetic index creator.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-6-28
 */
public class SyntheticIndexValidater {
	private static final String KEY_SPACE_NAME_PREFIX = "INDEX_";
	private static final String COLUMN_FAMILY_NAME = "HASH";

	private static Vector<Pair<String, Vector<String>>> dcInfo = null;

	static {
		Vector<String> nodes = null;

		dcInfo = new Vector<Pair<String, Vector<String>>>();

		try {
			/*
			 * Configuration hdfsConf = new Configuration(); FileSystem hdfs =
			 * FileSystem.get(hdfsConf); Path hdfsFile = new
			 * Path("cluster.conf"); FSDataInputStream is = hdfs.open(hdfsFile);
			 * Scanner in = new Scanner(is);
			 */
			Scanner in = new Scanner(new FileReader("/home/caoz/cluster.conf"));
			System.out.println("--------");
			while (in.hasNext()) {
				String ip = in.next();
				assert (in.hasNext());
				String dc = in.next();
				nodes = new Vector<String>();
				nodes.add(ip);
				dcInfo.add(new Pair<String, Vector<String>>(dc, nodes));
				System.out.println(ip + " " + dc);
			}
			System.out.println("--------");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		final String HOST = "localhost";
		final int PORT = 9160;

		if (args.length != 2) {
			System.err.println("Usage: <numberOfKeys> <sizeOfValues>");
			// 10000000 1024
			System.exit(1);
		}

		int numberOfKeys = Integer.valueOf(args[0]);
		int sizeOfValues = Integer.valueOf(args[1]);

		String indexName = "synthetic_" + numberOfKeys + "_" + sizeOfValues;

		String url = HOST + "," + PORT + "," + indexName;
		CassandraPartitionedIndexAccessor index = new CassandraPartitionedIndexAccessor(url);

		long totalTime = 0;

		for (int i = 0; i < numberOfKeys; i++) {
			long start = System.currentTimeMillis();
			Vector<Writable> values = index.get(new Text("" + i));
			long end = System.currentTimeMillis();
			totalTime += (end - start);

			if (i != 0 && i % 1000 == 0) {
				System.out.println("TotalTime : " + totalTime + " i: " + i + " Average: " + ((double)totalTime / (double) i));
				if (values.size() == 1) {
					System.out.println(i + ": " + ((Text) values.get(0)).toString().substring(0, 50));
				} else {
					System.out.println("No value returned error.");
					return;
				}
			}

		}
	}
}
