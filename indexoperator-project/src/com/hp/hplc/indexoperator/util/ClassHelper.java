package com.hp.hplc.indexoperator.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Helper class for IndexInput and IndexOutput
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-27
 */
public class ClassHelper {
	private static final byte INT_BYTE = 0;
	private static final byte LONG_BYTE = 1;
	private static final byte TEXT_BYTE = 2;
	
	public static byte class2byte(Class<? extends Writable> cls) {
		if (cls.equals(IntWritable.class))
			return (INT_BYTE);
		if (cls.equals(LongWritable.class))
			return (LONG_BYTE);
		if (cls.equals(Text.class))
			return (TEXT_BYTE);
		
		try {
			throw new Exception("Undefined class '" + cls.toString() + "'.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return ((byte) 0xFF);
	}
	
	public static Class<? extends Writable> byte2class(byte b) {
		if (b == INT_BYTE)
			return (IntWritable.class);
		if (b == LONG_BYTE)
			return (LongWritable.class);
		if (b == TEXT_BYTE)
			return (Text.class);
		
		try {
			throw new Exception("Undefined class byte '" + b + "'.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return (Writable.class);
	}
	
	public static void main(String[] args) {
		System.out.println(INT_BYTE + " " + LONG_BYTE + " " + TEXT_BYTE);
	}
}

