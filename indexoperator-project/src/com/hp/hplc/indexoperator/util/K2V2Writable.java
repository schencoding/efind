package com.hp.hplc.indexoperator.util;

import java.io.IOException;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.Serializable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ByteWritable;

import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

/**
 * Wrapper for <key, value, keys, values>.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-27
 */
public class K2V2Writable implements Writable, Serializable {
	private static final long serialVersionUID = 8210493855752261895L;

	private Writable key = null;
	private Writable value = null;
	private IndexInput keys = null;
	private IndexOutput values = null;
	
	private Class<? extends Writable> keyClass = null;
	private Class<? extends Writable> valueClass = null;
	
	public static final IndexInput EMPTY_INPUT = new IndexInput(0, null);
	public static final IndexOutput EMPTY_OUTPUT = new IndexOutput(EMPTY_INPUT, null);
	
	// For UT only
	public boolean equals(K2V2Writable another) {
		if (this.key == null || another.key == null)
			return (false);
		if (((IntWritable) this.key).get() !=
			((IntWritable) another.key).get())
			return (false);
		
		if (this.value == null || another.value == null)
			return (false);
		if (((IntWritable) this.value).get() !=
			((IntWritable) another.value).get())
			return (false);
		
		if (this.keys == null || another.keys == null)
			return (false);
		if (! this.keys.equals(another.keys))
			return (false);
		
		if (this.values == null || another.values == null)
			return (false);
		if (! this.values.equals(another.values))
			return (false);
		
		return (true);
	}
	
	public K2V2Writable() {
	}
	
	public K2V2Writable(Writable key, Writable value, IndexInput keys, IndexOutput values,
		Class<? extends Writable> keyClass, Class<? extends Writable> valueClass) {
		this.key = key;
		this.value = value;
		this.keys = keys;
		this.values = values;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}
	
	public Writable getKey() {
		return (key);
	}
	
	public void setKey(Writable key) {
		this.key = key;
	}
	
	public Writable getValue() {
		return (value);
	}
	
	public void setValue(Writable value) {
		this.value = value;
	}
	
	public IndexInput getKeys() {
		return (keys);
	}
	
	public void setKeys(IndexInput keys) {
		this.keys = keys;
	}
	
	public IndexOutput getValues() {
		return (values);
	}
	
	public void setValues(IndexOutput values) {
		this.values = values;
	}

	public void write(DataOutput out) throws IOException {
		ByteWritable b = new ByteWritable((byte) 0);
		
		//System.out.println("In K2V2Writable, keyClass = " + keyClass.getName());
		//System.out.println("In K2V2Writable, valueClass = " + valueClass.getName());
		
		b.set(ClassHelper.class2byte(keyClass));
		b.write(out);
		key.write(out);
		
		b.set(ClassHelper.class2byte(valueClass));
		b.write(out);
		value.write(out);

		keys.write(out);
		(values == null ? EMPTY_OUTPUT : values).write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		ByteWritable b = new ByteWritable((byte) 0);
		
		try {
			b.readFields(in);
			keyClass = ClassHelper.byte2class(b.get());
			key = keyClass.newInstance();
			key.readFields(in);
			
			b.readFields(in);
			valueClass = ClassHelper.byte2class(b.get());
			value = valueClass.newInstance();
			value.readFields(in);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
			
		keys = new IndexInput();
		keys.readFields(in);
		values = new IndexOutput();
		values.readFields(in);
	}
	
	public static K2V2Writable read(DataInput in) throws IOException {
		K2V2Writable obj = new K2V2Writable();
		obj.readFields(in);
		return (obj);
	}
}

