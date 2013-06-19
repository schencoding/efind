package com.hp.hplc.index.helper;

import java.io.FileReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

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
public class SyntheticIndexDataLoader {
	private static final String KEY_SPACE_NAME_PREFIX = "INDEX_";
	private static final String COLUMN_FAMILY_NAME = "HASH";
	
	private static Vector<Pair<String, Vector<String> > > dcInfo = null;

	static {
		Vector<String> nodes = null;
		
		dcInfo = new Vector<Pair<String, Vector<String> > >();
		
		try {
			/*
			Configuration hdfsConf = new Configuration();
		    FileSystem hdfs = FileSystem.get(hdfsConf);
		    Path hdfsFile = new Path("cluster.conf");
		    FSDataInputStream is = hdfs.open(hdfsFile);
		    Scanner in = new Scanner(is);
		    */
			Scanner in = new Scanner(new FileReader("/home/caoz/cluster.conf"));
			System.out.println("--------");
			while (in.hasNext()) {
				String ip = in.next();
				assert(in.hasNext());
				String dc = in.next();
				nodes = new Vector<String>();
				nodes.add(ip);
				dcInfo.add(new Pair<String, Vector<String> >(dc, nodes));
				System.out.println(ip + " " + dc);
			}
			System.out.println("--------");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static String random_string(int length) {
		final String dict = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
							"abcdefghijklmnopqrstuvwxyz" +
							"0123456789";
		char[] ans = new char [length];
		Random rand = new Random();
		
		for (int i = 0; i < length; i++)
			ans[i] = dict.charAt(rand.nextInt(dict.length()));
		
		return (new String(ans));
	}

	public static void main(String[] args) {
		final String HOST = "localhost";
		final int PORT = 9160;

		if (args.length != 6) {
			System.err.println("Usage: <numberOfKeys> <sizeOfValues> <numberOfPartitions> <partitionClass> <replicationFactor> <startKey>");
			// 10000000 1024 32 "org.apache.hadoop.mapred.lib.HashPartitioner" 1
			System.exit(1);
		}
		
		int numberOfKeys = Integer.valueOf(args[0]);
		int sizeOfValues = Integer.valueOf(args[1]);
		int numberOfPartitions = Integer.valueOf(args[2]);
		String partitionClass = args[3];
		int replicationFactor = Integer.valueOf(args[4]);
		int startKey = Integer.valueOf(args[5]);
		
		String indexName = "synthetic_" + numberOfKeys + "_" + sizeOfValues;
		
		
		String url = HOST + "," + PORT + "," + indexName;
		CassandraPartitionedIndexAccessor index = new CassandraPartitionedIndexAccessor(url);
		
		for (int i = startKey; i < numberOfKeys; i++) {
			index.insert(new Text("" + i), 0, new Text(random_string(sizeOfValues)));
			if ((i + 1) % 10000 == 0)
				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println(i + 1);
		}
	}
}
