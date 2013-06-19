package com.hp.hplc.util;

import java.io.FileReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import com.hp.hplc.index.CassandraPartitionedIndexAccessor;
import com.hp.hplc.index.__IndexAccessor;

public class CassandraLocalityTest {
	private static String hostname = null;
	
	static {
		try {
			InetAddress na = InetAddress.getLocalHost();
			hostname = na.getHostName();
			System.out.println("Hostname: " + hostname);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	static class Statistic {
		private String __name = null;
		private long __min, __max, __sum, __cnt;
		
		public Statistic(String name) {
			__name = name;
			__min = __max = __sum = __cnt = 0;
		}
		
		public void add(long v) {
			if (__cnt == 0)
				__min = __max = v;
			else {
				__min = Math.min(__min, v);
				__max = Math.max(__max, v);
			}
			
			__sum += v;
			__cnt++;
		}
		
		public void report() {
			System.out.println(__name + ": min " + __min + " max " + __max + " avg " +
				(double) __sum / __cnt);
		}
	}
	
	public static void main(String[] args) {
		__IndexAccessor index = new CassandraPartitionedIndexAccessor("localhost,9160,tpch_orders10G_32");
		
		System.out.println("Is Partitioned: " + index.isPartitioned());
		System.out.println("Partition Class: " + index.getPartitionClass());
		System.out.println("Number of Partitions: " + index.getNumberOfPartitions());
		
		List<String>[] locations = index.getPartitionLocations();
		boolean[] tag = new boolean [index.getNumberOfPartitions()];
		int i, j;
		
		for (i = 0; i < tag.length; i++) {
			tag[i] = false;
			for (j = 0; j < locations[i].size(); j++)
				if (locations[i].get(j).equals(hostname))
					tag[i] = true;
		}
		
		Partitioner<Writable, Writable> par = new HashPartitioner<Writable, Writable>();
		
		Statistic local = new Statistic("Local");
		Statistic remote = new Statistic("Remote");
		
		Random rand = new Random();
		
		try {
			/*
			String uri = "hdfs://localhost:9000/user/mdzfirst/input/lineitem10G/line-00000.tbl";
			Configuration conf = new Configuration();
		    FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
		    Path path = new Path(uri);
		    FSDataInputStream is = hdfs.open(path);
		    Scanner in = new Scanner(is);
		    */
			
			Scanner in = new Scanner(new FileReader("lineitem.tbl"));
		    
		    long cnt = 0;
		    
		    while (in.hasNext()) {
		    	String row = in.nextLine();
		    	String[] fields = row.split("\\|");
		    	Text key = new Text(fields[0]);
		    	int pid = par.getPartition(key, null, tag.length);
		    	
		    	if (true || rand.nextDouble() < 0.005) {
			    	long start = (new Date()).getTime();
			    	
			    	Vector<Writable> ans = index.get(key);
			    	assert(ans.size() == 1);
			    	
			    	long end = (new Date()).getTime();
			    	
			    	if (tag[pid])
			    		local.add(end - start);
			    	else
			    		remote.add(end - start);
		    	}
		    	
		    	cnt++;
		    	
		    	if (cnt % 10000 == 0)
		    		System.out.println(cnt);
		    	
		    	if (cnt >= 100000)
		    		break;
		    }
		    System.out.println(cnt);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		local.report();
		remote.report();
	}
}
