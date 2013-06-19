package com.hp.hplc.util;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

import org.apache.hadoop.mapred.lib.HashPartitioner;

public class InputPartitioner {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: java -jar InputPartition.jar <input> <number of partitions>");
			System.exit(1);
		}
		
		try {
			Scanner in = new Scanner(new FileReader(args[0]));
			int numberOfPartitions = Integer.valueOf(args[1]);
			HashPartitioner<String, String> par = new HashPartitioner<String, String> ();
			BufferedWriter[] out = new BufferedWriter [numberOfPartitions];
			int i;
			
			for (i = 0; i < numberOfPartitions; i++) {
				String filename = String.format("%s-%05d.tbl", args[0].substring(0, 4), i);
				out[i] = new BufferedWriter(new FileWriter(filename));
			}
			
			while (in.hasNext()) {
				String line = in.nextLine();
				String key = line.substring(0, line.indexOf('|'));
				int p = par.getPartition(key, null, numberOfPartitions);
				out[p].write(line);
			}
			
			for (i = 0; i < numberOfPartitions; i++) {
				out[i].flush();
				out[i].close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
