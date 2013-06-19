package com.hp.hplc.mr.driver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;


import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraPartitionTest {
	
	public static final String CAS_PARTITION_STORE_PATH_PREFIX = "";
	
	public static final String KEY_SPACE_NAME_PREFIX = "INDEX_";
	public static final String COLUMN_FAMILY_NAME = "HASH";
	public static final String COLUMN_NAME_PREFIX = "VALUE_";
	
	private String keyspace = "demo";
	private String columnFamily = "test";
	private String columnFamily1 = "test1";
	
	//private String host = "15.154.147.108";
	private String host = "localhost";
	private int port = 9160;
	public String name = "tpch_supplier";
	
	private TTransport tr = null;
	private TProtocol pr = null;
	private Cassandra.Client cli = null;
	private ColumnParent cf = null;
	private ColumnPath cp = null;
	
	
	
	
	
	public CassandraPartitionTest() {
		super();
		try {
			System.out.println("before connect.");
			tr = new TFramedTransport(new TSocket(this.host, this.port));
			pr = new TBinaryProtocol(tr);
			cli = new Cassandra.Client(pr);
			tr.open();
			System.out.println("after connect.");
			cli.set_keyspace(KEY_SPACE_NAME_PREFIX + this.name);
			cf = new ColumnParent(COLUMN_FAMILY_NAME);
			cp = new ColumnPath(COLUMN_FAMILY_NAME);
			/*cli.set_keyspace(keyspace);
			cf = new ColumnParent(columnFamily);
			cp = new ColumnPath(COLUMN_FAMILY_NAME);*/
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void test() throws InvalidRequestException, TException, UnsupportedEncodingException{
		List<TokenRange> list = cli.describe_ring(KEY_SPACE_NAME_PREFIX + this.name);
		Iterator<TokenRange> it = list.iterator();
		while(it.hasNext()){
			TokenRange tr = it.next();
			System.out.println(tr.getEnd_token().getBytes("UTF8").toString());
			System.out.println(tr);
		}
		
		System.out.println(cli.describe_partitioner());
		
		
		
		RandomPartitioner rp = new RandomPartitioner();
		
		OrderPreservingPartitioner opp = new OrderPreservingPartitioner();
		
		ByteBuffer key = ByteBufferUtil.bytes("1",Charset.forName("UTF8"));
		System.out.println(rp.getToken(key));
		
		System.out.println(opp.getClass().getName());
		System.out.println(opp.getToken(key));
	}
	
	public void writeOutRing(String keyspaceName) throws InvalidRequestException, TException, IOException{
		
		Configuration hdfsConf = new Configuration();
	    FileSystem hdfs = FileSystem.get(hdfsConf);
	    Path hdfsFile = new Path(CAS_PARTITION_STORE_PATH_PREFIX+keyspaceName+"_partition");
	    FSDataOutputStream out = hdfs.create(hdfsFile);
	    
	    
		List<TokenRange> list = cli.describe_ring(KEY_SPACE_NAME_PREFIX + this.name);
		Iterator<TokenRange> it = list.iterator();
		while(it.hasNext()){
			TokenRange tr = it.next();
			Text txt = new Text(tr.getEnd_token());
			txt.write(out);
			System.out.println(tr);
		}
		
		out.close();
	}
	
	public void writeOutRingTest(String keyspaceName) throws IOException{
		Configuration hdfsConf = new Configuration();
	    FileSystem hdfs = FileSystem.get(hdfsConf);
	    Path hdfsFile = new Path(CAS_PARTITION_STORE_PATH_PREFIX+keyspaceName+"_partition");
	    FSDataInputStream out = hdfs.open(hdfsFile);

	
		
	}

	/**
	 * @param args
	 * @throws TException 
	 * @throws InvalidRequestException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InvalidRequestException, TException, IOException {
		CassandraPartitionTest cpt = new CassandraPartitionTest();
		cpt.test();
		cpt.writeOutRing(KEY_SPACE_NAME_PREFIX + cpt.name);

	}

}
