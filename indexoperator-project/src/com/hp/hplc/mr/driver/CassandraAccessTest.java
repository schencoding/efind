package com.hp.hplc.mr.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class CassandraAccessTest {
	
	private static final String KEY_SPACE_NAME_1 = "INDEX_tpch_orders10G_32_5";
	private static final String COLUMN_FAMILY_NAME_1 = "HASH";
	private static final String KEY_SPACE_NAME_2 = "INDEX_tpch_orders10G_32_8";
	private static final String COLUMN_FAMILY_NAME_2 = "HASH";
	
	private String host1 = "localhost";
	private int port1 = 9160;
	
	private String host2 = "localhost";
	private int port2 = 9160;
	
	TFramedTransport tr1 = null;
	TFramedTransport tr2 = null;
	private TProtocol pr1 = null;
	private TProtocol pr2 = null;
	private Cassandra.Client cli1 = null;
	private Cassandra.Client cli2 = null;
	private ColumnParent cf1 = null;
	private ColumnParent cf2 = null;
	private ColumnPath cp1 = null;
	private ColumnPath cp2 = null;
	
	long time1 = 0;
	long time2 = 0;
	long counter1 = 0;
	long counter2 = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CassandraAccessTest test = new CassandraAccessTest();
		test.run(args[0]);
		

	}
	
	public CassandraAccessTest(){
		tr1 = new TFramedTransport(new TSocket(this.host1, this.port1));
		tr2 = new TFramedTransport(new TSocket(this.host2, this.port2));

		
		try {
			pr1 = new TBinaryProtocol(tr1);
			cli1 = new Cassandra.Client(pr1);
			tr1.open();

			cli1.set_keyspace(KEY_SPACE_NAME_1);
			cf1 = new ColumnParent(COLUMN_FAMILY_NAME_1);
			cp1 = new ColumnPath(COLUMN_FAMILY_NAME_1);

			pr2 = new TBinaryProtocol(tr2);
			cli2 = new Cassandra.Client(pr2);
			tr2.open();
			cli2.set_keyspace(KEY_SPACE_NAME_2);
			cf2 = new ColumnParent(COLUMN_FAMILY_NAME_2);
			cp2 = new ColumnPath(COLUMN_FAMILY_NAME_2);
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void run(String path){
		
		File file = new File(path);
		try {
			BufferedReader bw = new BufferedReader(new FileReader(file));

			String row = null;
			int counter = 0;
			while ((row = bw.readLine()) != null) {
				String[] fields = row.split("\\|");
				Text key = new Text(fields[0]);

				HashPartitioner partitioner = new HashPartitioner();
				int par = partitioner.getPartition(key, null, 32);
				//System.out.println("Key: " + key.toString() + " part: " + par);
				if (par == 5) {
					get1(key);
				} else if(par == 8)  {
					//cli2.set_keyspace(KEY_SPACE_NAME_2 + par);
					//cf2 = new ColumnParent(COLUMN_FAMILY_NAME_2);
					//cp2 = new ColumnPath(COLUMN_FAMILY_NAME_2);
					get2(key, par);
				}
				counter ++;
				
				if(counter%10000 == 0)
				{
					System.out.println("\nPartition 1: counter = " + counter1 + " time = " + time1 + ", per= " + (double)time1/(double)counter1);
					System.out.println("Partition 2: counter = " + counter2 + " time = " + time2 + ", per= " + (double)time2/(double)counter2);
				}
			}
			bw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		System.out.println("Partition 1: counter = " + counter1 + " time = " + time1 + ", per= " + (double)time1/(double)counter1);
		System.out.println("Partition 2: counter = " + counter2 + " time = " + time2 + ", per= " + (double)time2/(double)counter2);
	}
	
	
	
	public Vector<Writable> get1(Writable key) {
		assert(key != null);
		
		counter1 ++;
		
		long beginTime = System.currentTimeMillis();
		
		try {		
			cli1.set_keyspace("INDEX_tpch_orders10G_32_" + 5);
			SlicePredicate sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setStart(new byte[0]);
			sr.setFinish(new byte[0]);
			sr.setReversed(false);
			sr.setCount(1000000);
			sp.setSlice_range(sr);
			
			List<ColumnOrSuperColumn> coscs = cli1.get_slice(
				ByteBuffer.wrap(((Text) key).toString().getBytes("UTF-8")), cf1, sp, ConsistencyLevel.ONE);
			long endTime = System.currentTimeMillis();
			time1 += (endTime - beginTime);
			//System.out.println(coscs.size());
			
			if (coscs.size() > 0) {
				Iterator<ColumnOrSuperColumn> itr = coscs.iterator();
				Vector<Writable> res = new Vector<Writable>();
				while (itr.hasNext()) {
					ColumnOrSuperColumn socs = itr.next();
					//System.out.print(socs.isSetColumn() + " " + socs.isSetSuper_column());
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
	
	public Vector<Writable> get2(Writable key, int part) {
		assert(key != null);
		
		counter2 ++;
		
		long beginTime = System.currentTimeMillis();
		
		try {			
			cli2.set_keyspace("INDEX_tpch_orders10G_32_" + part);
			SlicePredicate sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setStart(new byte[0]);
			sr.setFinish(new byte[0]);
			sr.setReversed(false);
			sr.setCount(1000000);
			sp.setSlice_range(sr);
			List<ColumnOrSuperColumn> coscs = cli2.get_slice(
					ByteBuffer.wrap(((Text) key).toString().getBytes("UTF-8")), cf2, sp, ConsistencyLevel.ONE);
			long endTime = System.currentTimeMillis();
			time2 += (endTime - beginTime);
			System.out.print(coscs.size());
			
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

}
