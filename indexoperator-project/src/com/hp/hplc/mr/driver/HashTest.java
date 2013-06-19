package com.hp.hplc.mr.driver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class HashTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HashPartitioner partitioner = new HashPartitioner();
		String str = "45453453454";
		Text text = new Text(str);
		Writable w1 = text;
		int par1 = partitioner.getPartition(text.toString(), null, 32);
		int par2 = partitioner.getPartition(text, null, 32);
		int par3 = partitioner.getPartition(w1, null, 32);

		System.out.println("Par1:" + par1);
		System.out.println("Par2:" + par2);
		System.out.println("Par3:" + par3);
		
		HashPartitioner<Text, Text> p2 = new HashPartitioner<Text, Text>();
		int par22 = p2.getPartition(text, null, 32);
		System.out.println("par22:" + par22);
	}

}
