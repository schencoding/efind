package com.hp.hplc.index;

import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Vector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.hp.hplc.index.helper.CassandraIndexMeta;
import com.hp.hplc.util.Pair;

/**
 * Cassandra-based index, duplicate values supported.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-5-17
 */
public class CassandraPartitionedIndexAccessor extends __IndexAccessor implements Serializable {
	private static final long serialVersionUID = 1245467644401120520L;
	
	private static final String KEY_SPACE_NAME_PREFIX = "INDEX_";
	private static final String COLUMN_FAMILY_NAME = "HASH";
	private static final String COLUMN_NAME_PREFIX = "VALUE_";
	
	public String dummyStr = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	
	private String host = null;
	private int port = 0;
	private String name = null;
	
	private String partitionClass = null;
	private int numberOfPartitions = -1;
	private List<String>[] partitionLocations = null;
	
	private String[] keyspace_names = null;
	
	private boolean isMetaReady = false;
	private boolean isInitialized = false;
	
	private TTransport[] tr = null;
	private TProtocol[] pr = null;
	private Cassandra.Client[] cli = null;
	private ColumnParent cf = null;
	private ColumnPath cp = null;
	
	private Partitioner<Writable, Writable> par = null;

	public CassandraPartitionedIndexAccessor(String url) {
		super(url);
		this.setKeyClass(Text.class);
		this.setValueClass(Text.class);
		
		String[] strs = url.split(",");		
		
		this.host = strs[0];
		this.port = Integer.parseInt(strs[1]);
		this.name = strs[2];
	}
	
	public void initMeta() {
		assert(! isMetaReady);
		
		try {
			CassandraIndexMeta meta = new CassandraIndexMeta();
			
			partitionClass = meta.getPartitionClass(this.name);
			numberOfPartitions = meta.getNumberOfPartitions(this.name);
			partitionLocations = meta.getPartitionLocations(this.name);
			
			keyspace_names = new String [numberOfPartitions];
			for (int i = 0; i < numberOfPartitions; i++)
				keyspace_names[i] = KEY_SPACE_NAME_PREFIX + this.name + "_" + i;
			
			meta.close();
			
			isMetaReady = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void init() {
		assert(! isInitialized);
		
		if (! isMetaReady)
			initMeta();
		
		try {
			int i;
			
			/*
			tr = new TFramedTransport [numberOfPartitions];
			for (i = 0; i < numberOfPartitions; i++)
				tr[i] = new TFramedTransport(new TSocket(this.host, this.port));
			
			pr = new TBinaryProtocol [numberOfPartitions];
			for (i = 0; i < numberOfPartitions; i++)
				pr[i] = new TBinaryProtocol(tr[i]);
			*/
			
			tr = new TFramedTransport [1];
			tr[0] = new TFramedTransport(new TSocket(this.host, this.port));
			
			pr = new TBinaryProtocol [1];
			pr[0] = new TBinaryProtocol(tr[0]);
			
			/*
			cli = new Cassandra.Client [numberOfPartitions];
			for (i = 0; i < numberOfPartitions; i++)
				// cli[i] = new Cassandra.Client(pr[i]);
				cli[i] = new Cassandra.Client(pr[0]);
			*/
			
			cli = new Cassandra.Client [1];
			cli[0] = new Cassandra.Client(pr[0]);
			
			/*
			for (i = 0; i < numberOfPartitions; i++)
				tr[i].open();
			*/
			tr[0].open();
			
			/*
			for (i = 0; i < numberOfPartitions; i++)
				cli[i].set_keyspace(KEY_SPACE_NAME_PREFIX + this.name + "_" + i);
			*/
			
			cf = new ColumnParent(COLUMN_FAMILY_NAME);
			cp = new ColumnPath(COLUMN_FAMILY_NAME);
			
			par = (Partitioner<Writable, Writable>) Class.forName(partitionClass).newInstance();
			
			isInitialized = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public boolean isPartitioned() {
		return (true);
	}
	
	public String getPartitionClass() {
		if (! isMetaReady)
			initMeta();
		
		return (partitionClass);
	}
	
	public int getNumberOfPartitions() {
		if (! isMetaReady)
			initMeta();
		
		return (numberOfPartitions);
	}
	
	public List<String>[] getPartitionLocations() {
		if (! isMetaReady)
			initMeta();
		
		// return (partitionLocations);
		
		List<String>[] ans = new List [partitionLocations.length];
		for (int i = 0; i < partitionLocations.length; i++) {
			ans[i] = new Vector<String> ();
			Iterator<String> itr = partitionLocations[i].iterator();
			while (itr.hasNext()) {
				String ip = itr.next();
				String[] fields = ip.split("\\.");
				assert(fields.length == 4);
				int last = Integer.valueOf(fields[3]);
				String host = String.format("HPLCCL2BL%02d.hpl.hp.com", last - 160 + 1);
				ans[i].add(host);
			}
		}
		
		return (ans);
	}
	
	/*
	public void insert(Writable key, Writable value) {
		assert(key != null && value != null);
		
		try {
			if (! isInitialized)
				init();
			
			int idx = par.getPartition(
				((Text) key).toString(), ((Text) value).toString(), numberOfPartitions);
			
			SlicePredicate sp = new SlicePredicate();
			int cnt = cli[idx].get_count(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF8")), cf, sp, ConsistencyLevel.ONE);
			String columnName = COLUMN_NAME_PREFIX + cnt;
			Column col = new Column(ByteBuffer.wrap(columnName.getBytes("UTF8")));
			col.setValue(((Text) value).toString().getBytes("UTF8"));
			col.setTimestamp(System.currentTimeMillis());
			cli[idx].insert(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF8")), cf, col, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	*/
	
	public void insert(Writable key, int id, Writable value) {
		assert(key != null && value != null);
		
		try {
			if (! isInitialized)
				init();
			
			int idx = par.getPartition(key, value, numberOfPartitions);
			
			String columnName = COLUMN_NAME_PREFIX + id;
			Column col = new Column(ByteBuffer.wrap(columnName.getBytes("UTF8")));
			col.setValue(((Text) value).toString().getBytes("UTF8"));
			col.setTimestamp(System.currentTimeMillis());
			
			cli[0].set_keyspace(keyspace_names[idx]);
			idx = 0;
			
			cli[idx].insert(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF-8")), cf, col, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/*
	public void remove(Writable key) {
		assert(key != null);
		
		try {
			if (! isInitialized)
				init();
			
			int idx = par.getPartition(
				((Text) key).toString(), null, numberOfPartitions);
			
			long ts = System.currentTimeMillis();
			cli[idx].remove(ByteBuffer.wrap(((Text) key).toString().getBytes("UTF8")), cp, ts, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	*/

	@Override
	public Vector<Writable> get(Writable key) {
		//Vector<Writable> res = new Vector<Writable>();
		//res.add(new Text(dummyStr));
		//return res;
		assert(key != null);
		
		try {
			if (! isInitialized)
				init();
			
			int idx = par.getPartition(key, null, numberOfPartitions);

			SlicePredicate sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setStart(new byte[0]);
			sr.setFinish(new byte[0]);
			sr.setReversed(false);
			sr.setCount(1000000);
			sp.setSlice_range(sr);
			
			cli[0].set_keyspace(keyspace_names[idx]);
			idx = 0;
			
			List<ColumnOrSuperColumn> coscs = cli[idx].get_slice(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF-8")), cf, sp, ConsistencyLevel.ONE);
			if (coscs.size() > 0) {
				Iterator<ColumnOrSuperColumn> itr = coscs.iterator();
				Vector<Writable> res = new Vector<Writable>();
				while (itr.hasNext()) {
					ColumnOrSuperColumn socs = itr.next();
					// System.out.println(socs.isSetColumn() + " " + socs.isSetSuper_column());
					assert(socs.isSetColumn());
					Column col = socs.getColumn();
					res.add(new Text(new String(col.getValue())));
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
		try {
			if (! isInitialized)
				return;
			
			for (int i = 0; i < tr.length; i++)
				tr[i].close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
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
		final String HOST = "localhost";
		final int PORT = 9160;
		
		/*
		try {
			if (args.length != 1) {
				System.err.println("Usage: <index name>");
				System.exit(1);
			}
			
			String url = HOST + "," + PORT + "," + args[0];
			CassandraPartitionedIndexAccessor index = new CassandraPartitionedIndexAccessor(url);
			
			long total = 0;
			int i;
			for (i = 1; i <= 6000000; i++) {
				Writable key = new Text("" + i);
				Vector<Writable> values = index.get(key);
				if (values == null)
					continue;
				total += values.size();
				if (total % 1000 == 0)
					System.out.println(total);
			}
			System.out.println("total = " + total);
			
			/ *
			if (args.length != 1) {
				System.err.println("Usage: <keyspace>");
				System.exit(1);
			}
			
			TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
			TProtocol pr = new TBinaryProtocol(tr);
			Cassandra.Client cli = new Cassandra.Client(pr);
			tr.open();
			
			cli.set_keyspace(args[0]);
			
			ColumnParent cf = new ColumnParent(COLUMN_FAMILY_NAME);
			SlicePredicate sp = new SlicePredicate();
			KeyRange kr = new KeyRange();
			long total = 0, cnt;
			
			ByteBuffer bb = ByteBuffer.allocate(10);
			bb.put(((String) "HASH").getBytes("UTF8"));
			List<ByteBuffer> cn = new LinkedList<ByteBuffer> ();
			cn.add(bb);
			sp.setColumn_names(cn);
			
			kr.setStart_key(new byte [0]);
			kr.setEnd_key(new byte [0]);
			
			while (true) {
				List<KeySlice> list = cli.get_range_slices(cf, sp, kr, ConsistencyLevel.ONE);
				cnt = 0;
				Iterator<KeySlice> itr = list.iterator();
				KeySlice last = null;
				while (itr.hasNext()) {
					cnt++;
					last = itr.next();
				}
				if (cnt == 0)
					break;
				total += cnt;
				System.out.println(total);
				kr.setStart_key(last.getKey());
			}
			System.out.println("total = " + total);
			
			tr.close();
			* /
			
			return;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		*/
		
		/*
		if (args.length != 6) {
			System.err.println("Usage: <table> <file> <keyIndex> <numberOfPartitions> <partitionClass> <replicationFactor>");
			// "table" "input" 0 10 "org.apache.hadoop.mapred.lib.HashPartitioner" 1
			System.exit(1);
		}
		*/
		
		if (args.length != 4) {
			System.err.println("Usage: <table> <file> <keyIndex> <valueIdIndex>");
			System.exit(1);
		}
		
		String table = args[0];
		String indexName = "tpch_" + table;
		String file = args[1];
		String[] splits = args[2].split(",");
		Vector<Integer> keyIndex = new Vector<Integer>();
		for (int i = 0; i < splits.length; i++)
			keyIndex.add(Integer.valueOf(splits[i]));
		int valueIdIndex = Integer.valueOf(args[3]).intValue();

		/*
		String table = args[0];
		String indexName = "tpch_" + table;
		String file = args[1];
		int keyIndex = Integer.valueOf(args[2]);
		int numberOfPartitions = Integer.valueOf(args[3]);
		String partitionClass = args[4];
		int replicationFactor = Integer.valueOf(args[5]);
		Map<String, String> op = new LinkedHashMap<String, String> ();

		try {
			TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
			TProtocol pr = new TBinaryProtocol(tr);
			Cassandra.Client cli = new Cassandra.Client(pr);
			tr.open();
			
			CassandraIndexMeta meta = new CassandraIndexMeta();
			meta.setPartitionClass(indexName, partitionClass);
			meta.setNumberOfPartitions(indexName, numberOfPartitions);
			for (int i = 0; i < numberOfPartitions; i++) {
				Thread.sleep(10 * 1000);
				
				assert(dcInfo.size() >= replicationFactor);		// TODO: Now a dc can only have one replica.
				Collections.shuffle(dcInfo);
				op.clear();
				for (int j = 0; j < replicationFactor; j++) {
					op.put(dcInfo.get(j).first, "1");
					for (String location: dcInfo.get(j).second)
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
		
		boolean debug = false;
		if (debug) {
			String url = HOST + "," + PORT + "," + indexName;
			CassandraPartitionedIndexAccessor index = new CassandraPartitionedIndexAccessor(url);
			System.out.println(index.getNumberOfPartitions());
			System.out.println(index.getPartitionClass());
			List<String>[] arr = index.getPartitionLocations();
			for (int i = 0; i < arr.length; i++) {
				System.out.print("Partition " + i + ":");
				Iterator<String> itr = arr[i].iterator();
				while (itr.hasNext())
					System.out.print(" " + itr.next());
				System.out.println("");
			}
			return;
		}
		*/
		
		try {
			// assert(table.equals("customer") || table.equals("orders") || table.equals("supplier"));
			Scanner in = new Scanner(new File(file));
			String url = HOST + "," + PORT + "," + indexName;
			CassandraPartitionedIndexAccessor index = new CassandraPartitionedIndexAccessor(url);
			int cnt = 0;

			while (in.hasNext()) {
				String line = in.nextLine();
				String[] fields = line.split("\\|");
				assert(fields.length > 0);
				// Text key = new Text(fields[keyIndex]);
				String __key = "";
				for (int i = 0; i < keyIndex.size(); i++) {
					if (i > 0)
						__key += "|";
					__key += fields[keyIndex.get(i).intValue()];
				}
				Text key = new Text(__key);
				Text value = new Text(line);
				if (valueIdIndex < 0)
					index.insert(key, 0, value);
				else {
					assert(valueIdIndex < fields.length);
					int valueId = Integer.valueOf(fields[valueIdIndex]).intValue();
					index.insert(key, valueId, value);
				}

				cnt++;
				if (cnt % 10000 == 0)
					System.out.println(cnt);
			}
			System.out.println(cnt);
			
			System.out.println("--------");
			System.out.println(index.getNumberOfPartitions() + " partitions.");
			List<String>[] locations = index.getPartitionLocations();
			for (int i = 0; i < locations.length; i++) {
				System.out.print("Partition " + i + ":");
				Iterator<String> itr = locations[i].iterator();
				while (itr.hasNext())
					System.out.print(" " + itr.next());
				System.out.println("");
			}
			System.out.println("--------");

			index.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public Class<? extends Writable> getKeyClass() {
		return (Text.class);
	}

	@Override
	public Class<? extends Writable> getValueClass() {
		return (Text.class);
	}
}
