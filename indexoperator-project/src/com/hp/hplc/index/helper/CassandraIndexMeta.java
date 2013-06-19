package com.hp.hplc.index.helper;

import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.hp.hplc.util.Pair;

public class CassandraIndexMeta {
	private static final String HOST = "localhost";
	private static final int PORT = 9160;
	
	private static final String KEY_SPACE_NAME = "CASSANDRA_INDEX_META";
	private static final String COLUMN_FAMILY_NAME = "CASSANDRA_INDEX_META_COLUMN_FAMILY";
	private static final String COLUMN_NAME_PREFIX = "VALUE_";
	private static final int REPLICATION_FACTOR = 1;
	
	private TTransport tr = null;
	private TProtocol pr = null;
	private Cassandra.Client cli = null;
	private ColumnParent cf = null;
	private ColumnPath cp = null;
	
	public CassandraIndexMeta() {
		try {
			tr = new TFramedTransport(new TSocket(HOST, PORT));
			pr = new TBinaryProtocol(tr);
			cli = new Cassandra.Client(pr);
			tr.open();
			cli.set_keyspace(KEY_SPACE_NAME);
			cf = new ColumnParent(COLUMN_FAMILY_NAME);
			cp = new ColumnPath(COLUMN_FAMILY_NAME);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void insert(String key, String value) {
		assert(key != null && value != null);
		
		while (true) {
			try {
				SlicePredicate sp = new SlicePredicate();
				int cnt = cli.get_count(ByteBuffer.wrap(key.getBytes("UTF8")), cf, sp, ConsistencyLevel.ONE);
				String columnName = COLUMN_NAME_PREFIX + cnt;
				Column col = new Column(ByteBuffer.wrap(columnName.getBytes("UTF8")));
				col.setValue(value.getBytes("UTF8"));
				col.setTimestamp(System.currentTimeMillis());
				cli.insert(ByteBuffer.wrap(key.getBytes("UTF8")), cf, col, ConsistencyLevel.ALL);
			} catch (TimedOutException e) {
				System.out.println("Retrying ...");
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			break;
		}
	}
	
	private void insert(String key, int id, String value) {
		assert(key != null && value != null);
		
		while (true) {
			try {
				String columnName = COLUMN_NAME_PREFIX + id;
				Column col = new Column(ByteBuffer.wrap(columnName.getBytes("UTF8")));
				col.setValue(value.getBytes("UTF8"));
				col.setTimestamp(System.currentTimeMillis());
				cli.insert(ByteBuffer.wrap(key.getBytes("UTF8")), cf, col, ConsistencyLevel.ALL);
			} catch (TimedOutException e) {
				System.out.println("Retrying ...");
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			break;
		}
	}

	private void remove(String key) {
		assert(key != null);
		
		try {
			long ts = System.currentTimeMillis();
			cli.remove(ByteBuffer.wrap(key.getBytes("UTF8")), cp, ts, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private Vector<String> get(String key) {
		assert(key != null);
		
		try {
			SlicePredicate sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setStart(new byte[0]);
			sr.setFinish(new byte[0]);
			sp.setSlice_range(sr);
			List<ColumnOrSuperColumn> coscs = cli.get_slice(
				ByteBuffer.wrap(key.getBytes("UTF8")), cf, sp, ConsistencyLevel.ONE);
			if (coscs.size() > 0) {
				Iterator<ColumnOrSuperColumn> itr = coscs.iterator();
				Vector<String> res = new Vector<String>();
				while (itr.hasNext()) {
					ColumnOrSuperColumn socs = itr.next();
					// System.out.println(socs.isSetColumn() + " " + socs.isSetSuper_column());
					assert(socs.isSetColumn());
					Column col = socs.getColumn();
					res.add(new String(col.getValue()));
				}
				return (res);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return (null);
	}
	
	public void close() {
		tr.close();
	}
	
	public void setPartitionClass(String index, String partitionClass) {
		insert("PARTITION_CLASS_" + index, 0, partitionClass);
	}
	
	public String getPartitionClass(String index) {
		Vector<String> v = get("PARTITION_CLASS_" + index);
		assert(v.size() == 1);
		return (v.get(0));
	}
	
	public void setNumberOfPartitions(String index, int numberOfPartitions) {
		insert("NUMBER_OF_PARTITIONS_" + index, 0, "" + numberOfPartitions);
	}
	
	public int getNumberOfPartitions(String index) {
		Vector<String> v = get("NUMBER_OF_PARTITIONS_" + index);
		assert(v.size() == 1);
		return (Integer.valueOf(v.get(0)));
	}
	
	public void addPartitionLocation(String index, int partition, String location) {
		insert("PARTITION_LOCATIONS_" + index + "_" + partition, location);
	}

	public List<String>[] getPartitionLocations(String index) {
		int i, numberOfPartition = getNumberOfPartitions(index);
		List<String>[] ans = new List [numberOfPartition];
		for (i = 0; i < numberOfPartition; i++)
			ans[i] = get("PARTITION_LOCATIONS_" + index + "_" + i);
		return (ans);
	}
	
	private static Vector<Pair<String, Vector<String> > > dcInfo = null;

	static {
		Vector<String> nodes = null;
		
		dcInfo = new Vector<Pair<String, Vector<String> > >();
		
		
		/*nodes = new Vector<String>();
		nodes.add("15.154.147.160");
		dcInfo.add(new Pair<String, Vector<String> >("dc160", nodes));
		
		nodes = new Vector<String>();
		nodes.add("15.154.147.161");
		dcInfo.add(new Pair<String, Vector<String> >("dc161", nodes));
		
		nodes = new Vector<String>();
		nodes.add("15.154.147.162");
		dcInfo.add(new Pair<String, Vector<String> >("dc162", nodes));
		
		nodes = new Vector<String>();
		nodes.add("15.154.147.163");
		dcInfo.add(new Pair<String, Vector<String> >("dc163", nodes));*/
		
		
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

	public static void main(String[] args) {
		try {
			TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
			TProtocol pr = new TBinaryProtocol(tr);
			Cassandra.Client cli = new Cassandra.Client(pr);
			tr.open();
			
			List<CfDef> cf = new LinkedList<CfDef>();
			cf.add(new CfDef(KEY_SPACE_NAME, COLUMN_FAMILY_NAME));
			
			Map<String, String> op = new LinkedHashMap<String, String> ();
			for (int i = 0; i < dcInfo.size(); i++)
				op.put(dcInfo.get(i).first, String.valueOf(REPLICATION_FACTOR));
			
			KsDef ks = new KsDef();
			ks.setName(KEY_SPACE_NAME);
			ks.setStrategy_class("org.apache.cassandra.locator.NetworkTopologyStrategy");
			ks.setStrategy_options(op);
			ks.setCf_defs(cf);
			cli.system_add_keyspace(ks);
			
			tr.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
