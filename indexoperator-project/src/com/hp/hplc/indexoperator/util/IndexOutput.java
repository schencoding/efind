package com.hp.hplc.indexoperator.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;
import java.util.Vector;
import java.util.Iterator;

import com.hp.hplc.indexoperator.util.IndexInput;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

/**
 * Collector for index values.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public class IndexOutput implements Writable {
	private Vector<Writable>[][] values = null;
	private Class<? extends Writable>[] valueClasses = null;
	
	// For UT only
	public boolean equals(IndexOutput another) {
		int i, j, k;
		
		if (this.values == null || another.values == null)
			return (false);
		if (this.values.length != another.values.length)
			return (false);
		for (i = 0; i < this.values.length; i++) {
			if (this.values[i] == null || another.values[i] == null)
				return (false);
			if (this.values[i].length != another.values[i].length)
				return (false);
			for (j = 0; j < this.values[i].length; j++) {
				if (this.values[i][j] == null || another.values[i][j] == null)
					return (false);
				if (this.values[i][j].size() != another.values[i][j].size())
					return (false);
				for (k = 0; k < this.values[i][j].size(); k++) {
					if (this.values[i][j].get(k) == null || another.values[i][j].get(k) == null)
						return (false);
					if (((IntWritable) this.values[i][j].get(k)).get() != 
						((IntWritable) another.values[i][j].get(k)).get())
						return (false);
				}
			}
		}
		
		if (this.valueClasses == null || another.valueClasses == null)
			return (false);		
		if (this.valueClasses.length != another.valueClasses.length)
			return (false);		
		for (i = 0; i < this.valueClasses.length; i++) {
			if (this.valueClasses[i] == null || another.valueClasses[i] == null)
				return (false);
			if (! this.valueClasses[i].equals(another.valueClasses[i]))
				return (false);
		}
		
		return (true);
	}
	
	public IndexOutput() {
	}

	public IndexOutput(IndexInput input, Class<? extends Writable>[] valueClasses) {
		int i, j;
		Vector<Writable>[] in = input.getInternal();
		
		assert(in.length == valueClasses.length);
		
		values = new Vector [in.length][];
		for (i = 0; i < values.length; i++) {
			values[i] = new Vector [in[i].size()];
			for (j = 0; j < values[i].length; j++)
				values[i][j] = new Vector<Writable>();
		}
		this.valueClasses = valueClasses;
	}
	
	public int size(int indexID) {
		return (values[indexID].length);
	}
	
	public int size(int indexID, int keyID) {
		return (values[indexID][keyID].size());
	}
	
	public void put(int indexID, int keyID, Writable value) {
		if (value != null)
			values[indexID][keyID].add(value);
	}
	
	public void put(int indexID, int keyID, Vector<Writable> values) {
		if (values != null)
			this.values[indexID][keyID].addAll(values);
	}
	
	public Iterator<Writable> iterator(int indexID, int keyID) {
		return (values[indexID][keyID].iterator());
	}
	
	public Vector<Writable>[][] getInternal() {
		return (values);
	}
	
	public void write(DataOutput out) throws IOException {
		int i, j, k;
		IntWritable n = new IntWritable(0);
		ByteWritable b = new ByteWritable((byte) 0);

		n.set(values.length);
		n.write(out);
		for (i = 0; i < values.length; i++) {
			n.set(values[i].length);
			n.write(out);
			b.set(ClassHelper.class2byte(valueClasses[i]));
			b.write(out);
			for (j = 0; j < values[i].length; j++) {
				n.set(values[i][j].size());
				n.write(out);
				for (k = 0; k < values[i][j].size(); k++)
					values[i][j].get(k).write(out);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		int i, j, k;
		IntWritable n = new IntWritable(0);
		ByteWritable b = new ByteWritable((byte) 0);
		
		n.readFields(in);
		values = new Vector [n.get()][];
		valueClasses = new Class [n.get()];
		for (i = 0; i < values.length; i++) {
			n.readFields(in);
			b.readFields(in);
			values[i] = new Vector [n.get()];
			valueClasses[i] = ClassHelper.byte2class(b.get());
			for (j = 0; j < values[i].length; j++) {
				n.readFields(in);
				values[i][j] = new Vector<Writable> ();
				for (k = 0; k < n.get(); k++) {
					Writable instance = null;
					try {
						instance = valueClasses[i].newInstance();
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
					instance.readFields(in);
					values[i][j].add(instance);
				}
			}
		}
	}

	public static IndexOutput read(DataInput in) throws IOException {
		IndexOutput obj = new IndexOutput();
		obj.readFields(in);
		return (obj);
	}

	@Override
	public String toString() {
		return "IndexOutput [values=" + Arrays.toString(values) + ", valueClasses=" + Arrays.toString(valueClasses)
				+ "]";
	}
	
	
}
