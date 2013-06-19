package com.hp.hplc.index.helper;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SyntheticInputCreator {
	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Usage: <numberOfRows> <sizeOfValues> <numberOfKeys> <fileName>");
			// 10000000 100 10000000
			System.exit(1);
		}
		
		int numberOfRows = Integer.valueOf(args[0]);
		int sizeOfValues = Integer.valueOf(args[1]);
		int numberOfKeys = Integer.valueOf(args[2]);
		int i, j;
		
		Random rand = new Random();
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(args[3]);
			bw = new BufferedWriter(fw);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (true) {
			for (i = 0; i < numberOfRows; i++) {
				String row = "" + rand.nextInt(numberOfKeys) + ",";
				row += SyntheticInputCreator.random_string(sizeOfValues);
				try {
					bw.write(row);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				//System.out.println(row);
			}
			try {
				bw.flush();
				fw.flush();
				fw.close();
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} else {
			double[] prob = new double [numberOfKeys];
			double sum = 0;
			for (i = 0; i < numberOfKeys; i++) {
				prob[i] = 1.0 / (i + 1);
				sum += prob[i];
			}
			for (i = 0; i < numberOfKeys; i++)
				prob[i] /= sum;
			for (i = 0; i < numberOfRows; i++) {
				double p = rand.nextDouble();
				for (j = 0; j < numberOfKeys; j++) {
					if (p <= prob[j])
						break;
					p -= prob[j];
				}
				assert(j < numberOfKeys);
				String row = "" + j + ",";
				row += SyntheticInputCreator.random_string(sizeOfValues);
				System.out.println(row);
			}
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
}
