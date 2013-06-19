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

public class SyntheticIndexKeySpaceCreator {
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

		if (args.length != 5) {
			System.err
					.println("Usage: <numberOfKeys> <sizeOfValues> <numberOfPartitions> <partitionClass> <replicationFactor>");
			// 10000000 1024 32 "org.apache.hadoop.mapred.lib.HashPartitioner" 1
			System.exit(1);
		}

		int numberOfKeys = Integer.valueOf(args[0]);
		int sizeOfValues = Integer.valueOf(args[1]);
		int numberOfPartitions = Integer.valueOf(args[2]);
		String partitionClass = args[3];
		int replicationFactor = Integer.valueOf(args[4]);

		String indexName = "synthetic_" + numberOfKeys + "_" + sizeOfValues;

		Map<String, String> op = new LinkedHashMap<String, String>();

		try {
			TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
			TProtocol pr = new TBinaryProtocol(tr);
			Cassandra.Client cli = new Cassandra.Client(pr);
			tr.open();

			CassandraIndexMeta meta = new CassandraIndexMeta();
			meta.setPartitionClass(indexName, partitionClass);
			meta.setNumberOfPartitions(indexName, numberOfPartitions);
			for (int i = 0; i < numberOfPartitions; i++) {
				Thread.sleep(3 * 1000);

				assert (dcInfo.size() >= replicationFactor); // TODO: Now a dc
																// can only have
																// one replica.
				Collections.shuffle(dcInfo);
				op.clear();
				for (int j = 0; j < replicationFactor; j++) {
					op.put(dcInfo.get(j).first, "1");
					for (String location : dcInfo.get(j).second)
						meta.addPartitionLocation(indexName, i, location);
				}

				List<CfDef> cf = new LinkedList<CfDef>();
				cf.add(new CfDef(KEY_SPACE_NAME_PREFIX + indexName + "_" + i, COLUMN_FAMILY_NAME));

				KsDef ks = new KsDef();
				ks.setName(KEY_SPACE_NAME_PREFIX + indexName + "_" + i);
				ks.setStrategy_class("org.apache.cassandra.locator.NetworkTopologyStrategy");
				ks.setStrategy_options(op);
				ks.setCf_defs(cf);
				cli.system_add_keyspace(ks);
			}

			tr.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		System.out.println(numberOfPartitions + " partitions created.");

		return;
	}
}
