package com.hp.hplc.index;

import java.io.File;
import java.io.Serializable;
import java.util.Vector;

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
import java.util.Vector;

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
public class CassandraIndexAccessor extends __IndexAccessor implements Serializable {
	private static final long serialVersionUID = 1245467644401120520L;
	
	private static final String KEY_SPACE_NAME_PREFIX = "INDEX_";
	private static final String COLUMN_FAMILY_NAME = "HASH";
	private static final String COLUMN_NAME_PREFIX = "VALUE_";
	
	public String dummyStr = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	
	private String host = null;
	private int port = 0;
	private String name = null;
	
	private TTransport tr = null;
	private TProtocol pr = null;
	private Cassandra.Client cli = null;
	private ColumnParent cf = null;
	private ColumnPath cp = null;
	
	private boolean isInitialized = false;

	public CassandraIndexAccessor(String url) {
		super(url);
		this.setKeyClass(Text.class);
		this.setValueClass(Text.class);
		
		String[] strs = url.split(",");		
		
		this.host = strs[0];
		this.port = Integer.parseInt(strs[1]);
		this.name = strs[2];
		
		try {
			tr = new TFramedTransport(new TSocket(this.host, this.port));
			pr = new TBinaryProtocol(tr);
			cli = new Cassandra.Client(pr);
			tr.open();
			cli.set_keyspace(KEY_SPACE_NAME_PREFIX + this.name);
			cf = new ColumnParent(COLUMN_FAMILY_NAME);
			cp = new ColumnPath(COLUMN_FAMILY_NAME);
			
			isInitialized = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void insert(Writable key, Writable value) {
		assert(key != null && value != null);
		
		try {
			if (! isInitialized)
				throw new Exception("The index accessor has not been initialized.");
			
			SlicePredicate sp = new SlicePredicate();
			int cnt = cli.get_count(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF8")), cf, sp, ConsistencyLevel.ONE);
			String columnName = COLUMN_NAME_PREFIX + cnt;
			Column col = new Column(ByteBuffer.wrap(columnName.getBytes()));
			col.setValue(((Text) value).toString().getBytes());
			col.setTimestamp(System.currentTimeMillis());
			cli.insert(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF8")), cf, col, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void remove(Writable key) {
		assert(key != null);
		
		try {
			if (! isInitialized)
				throw new Exception("The index accessor has not been initialized.");
			
			long ts = System.currentTimeMillis();
			cli.remove(ByteBuffer.wrap(((Text) key).toString().getBytes()), cp, ts, ConsistencyLevel.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public Vector<Writable> get(Writable key) {
		//Vector<Writable> res = new Vector<Writable>();
		//res.add(new Text(dummyStr));
		//return res;
		assert(key != null);
		
		try {
			if (! isInitialized)
				throw new Exception("The index accessor has not been initialized.");
			
			SlicePredicate sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setStart(new byte[0]);
			sr.setFinish(new byte[0]);
			sp.setSlice_range(sr);
			List<ColumnOrSuperColumn> coscs = cli.get_slice(
				ByteBuffer.wrap(((Text) key).toString().getBytes()), cf, sp, ConsistencyLevel.ONE);
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
				throw new Exception("The index accessor has not been initialized.");
			
			tr.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		final String HOST = "localhost";
		final int PORT = 9160;
		
		if (args.length != 4) {
			System.err.println("Usage: <table> <file> <keyIndex> <replicationFactor>");
			// "table" "input" 0 1
			System.exit(1);
		}

		String table = args[0];
		String indexName = "tpch_" + table;
		String file = args[1];
		int keyIndex = Integer.valueOf(args[2]);
		int replicationFactor = Integer.valueOf(args[3]);
		Map<String, String> op = new LinkedHashMap<String, String> ();

		try {
			TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
			TProtocol pr = new TBinaryProtocol(tr);
			Cassandra.Client cli = new Cassandra.Client(pr);
			tr.open();
			
			List<CfDef> cf = new LinkedList<CfDef>();
			cf.add(new CfDef(KEY_SPACE_NAME_PREFIX + indexName, COLUMN_FAMILY_NAME));
			
			op.put("replication_factor", String.valueOf(replicationFactor));
				
			KsDef ks = new KsDef();
			ks.setName(KEY_SPACE_NAME_PREFIX + indexName);
			ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
			ks.setStrategy_options(op);
			ks.setCf_defs(cf);
			cli.system_add_keyspace(ks);

			tr.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		boolean debug = true;
		if (debug) {
			return;
		}
		
		try {
			assert(table.equals("customer") || table.equals("orders") || table.equals("supplier"));
			Scanner in = new Scanner(new File(file));		
			String url = HOST + "," + PORT + "," + indexName;
			CassandraIndexAccessor index = new CassandraIndexAccessor(url);
			int cnt = 0;

			while (in.hasNext()) {
				String line = in.nextLine();
				String[] fields = line.split("\\|");
				assert(fields.length > 0);
				Text key = new Text(fields[keyIndex]);
				Text value = new Text(line);
				index.insert(key, value);

				cnt++;
				if (cnt % 10000 == 0)
					System.out.println(cnt);
			}
			System.out.println(cnt);

			index.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public Class<? extends Writable> getKeyClass() {
		return Text.class;
	}

	@Override
	public Class<? extends Writable> getValueClass() {
		return Text.class;
	}
}
