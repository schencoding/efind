package com.hp.hplc.indexoperator.util;

import java.lang.Exception;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;
import java.util.Vector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ByteWritable;

/**
 * Collector for index keys.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public class IndexInput implements Writable {
	private Vector<Writable>[] keys = null;
	private Class<? extends Writable>[] keyClasses = null;
	
	
	

	// For UT only
	public boolean equals(IndexInput another) {
		int i, j;
		
		if (this.keys == null || another.keys == null)
			return (false);		
		if (this.keys.length != another.keys.length)
			return (false);		
		for (i = 0; i < this.keys.length; i++) {
			if (this.keys[i] == null || another.keys[i] == null)
				return (false);
			if (this.keys[i].size() != another.keys[i].size())
				return (false);
			for (j = 0; j < this.keys[i].size(); j++) {
				if (this.keys[i] == null || another.keys[i] == null)
					return (false);
				if (((IntWritable) this.keys[i].get(j)).get() !=
					((IntWritable) another.keys[i].get(j)).get())
					return (false);
			}
		}
		
		if (this.keyClasses == null || another.keyClasses == null)
			return (false);		
		if (this.keyClasses.length != another.keyClasses.length)
			return (false);		
		for (i = 0; i < this.keyClasses.length; i++) {
			if (this.keyClasses[i] == null || another.keyClasses[i] == null)
				return (false);
			if (! this.keyClasses[i].equals(another.keyClasses[i]))
				return (false);
		}
		
		return (true);
	}

	public IndexInput() {
	}

	public IndexInput(int indexNumber, Class<? extends Writable>[] keyClasses) {
		int i;
		
		assert(indexNumber == keyClasses.length);
		
		keys = new Vector [indexNumber];
		for (i = 0; i < indexNumber; i++)
			keys[i] = new Vector<Writable>();
		this.keyClasses = keyClasses;
	}
	
	public int size(int indexID) {
		return (keys[indexID].size());
	}
	
	public void put(int indexID, Writable key) {
		if (key != null)
			keys[indexID].add(key);					// Leave the range check to Java
	}
	
	public Writable get(int indexID, int keyID) {
		return (keys[indexID].get(keyID));
	}
	
	public Vector<Writable>[] getInternal() {
		return (keys);
	}
	
	public void write(DataOutput out) throws IOException {
		int i, j;
		IntWritable n = new IntWritable(0);
		ByteWritable b = new ByteWritable((byte) 0);

		n.set(keys.length);
		n.write(out);
		for (i = 0; i < keys.length; i++) {
			n.set(keys[i].size());
			n.write(out);
			b.set(ClassHelper.class2byte(keyClasses[i]));
			b.write(out);
			for (j = 0; j < keys[i].size(); j++)
				keys[i].get(j).write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		int i, j;
		IntWritable n = new IntWritable(0);
		ByteWritable b = new ByteWritable((byte) 0);
		
		n.readFields(in);
		keys = new Vector [n.get()];
		keyClasses = new Class [n.get()];
		for (i = 0; i < keys.length; i++) {
			n.readFields(in);
			b.readFields(in);
			keys[i] = new Vector<Writable>();
			keyClasses[i] = ClassHelper.byte2class(b.get());
			for (j = 0; j < n.get(); j++) {
				Writable instance = null;
				try {
					instance = keyClasses[i].newInstance();
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				instance.readFields(in);
				keys[i].add(instance);
			}
		}
	}

	public static IndexInput read(DataInput in) throws IOException {
		IndexInput obj = new IndexInput();
		obj.readFields(in);
		return (obj);
	}

	@Override
	public String toString() {
		return "IndexInput [keys=" + Arrays.toString(keys) + ", keyClasses=" + Arrays.toString(keyClasses) + "]";
	}
	
	
}

