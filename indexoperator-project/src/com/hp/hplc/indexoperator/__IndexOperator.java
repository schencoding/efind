package com.hp.hplc.indexoperator;

import java.io.Serializable;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.index.__IndexAccessor;

/**
 * Base class for index operators.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public abstract class __IndexOperator implements Serializable {
	private static final long serialVersionUID = 2037884857053226592L;
	
	private Vector<__IndexAccessor> accessors = null;
	private Vector<String> names = null;
	private Vector<String> urls = null;
	
	private Class<? extends Writable> inputKeyClass = Text.class;
	private Class<? extends Writable> inputValueClass = Text.class;
	private Class<? extends Writable> preProKeyClass ;
	private Class<? extends Writable> preProValueClass ;
	private Class<? extends Writable> outputKeyClass = Text.class;
	private Class<? extends Writable> outputValueClass = Text.class;

	public __IndexOperator() {
		accessors = new Vector<__IndexAccessor>();
		names = new Vector<String>();
		urls = new Vector<String>();
	}
	
	public void close() {
		for (int i = 0; i < accessors.size(); i++)
			accessors.get(i).close();
	}

	public void addIndex(String name, String url) {
		try {
			names.add(name);
			urls.add(url);
			
			Class<?> cls = Class.forName(name);
			if (cls == null)
				throw new ClassNotFoundException();
			Constructor con = cls.getConstructor(String.class);
			if (con == null)
				throw new Exception();
			__IndexAccessor accessor = (__IndexAccessor) con.newInstance(url);
			accessors.add(accessor);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public int size() {
		return (accessors.size());
	}
	
	public Vector<__IndexAccessor> getInternal() {
		return (accessors);
	}
	
	public __IndexAccessor getIndexAccessor(int index){
		return accessors.get(index);
	}
	
	public Vector<String> getNames() {
		return (names);
	}
	
	public Vector<String> getUrls() {
		return (urls);
	}
	
	public String getAccessStr(int i){
		return urls.get(i);
	}
	
	public abstract boolean preprocess(Writable key, Writable value, IndexInput keys);
	
	public abstract void postprocess(Writable key, Writable value, IndexInput keys, IndexOutput values,
		OutputCollector<Writable, Writable> output);

	public Class<?  extends Writable> getInputKeyClass() {
		return inputKeyClass;
	}

	public void setInputKeyClass(Class<? extends Writable> inputKeyClass) {
		this.inputKeyClass = inputKeyClass;
	}

	public Class<? extends Writable> getInputValueClass() {
		return inputValueClass;
	}

	public void setInputValueClass(Class<? extends Writable> inputValueClass) {
		this.inputValueClass = inputValueClass;
	}

	public Class<? extends Writable> getPreProKeyClass() {
		if(preProKeyClass == null){
			return inputKeyClass;
		}else{
			return preProKeyClass;
		}
	}

	public void setPreProKeyClass(Class<? extends Writable> preProKeyClass) {
		this.preProKeyClass = preProKeyClass;
	}

	public Class<? extends Writable> getPreProValueClass() {
		if(preProValueClass == null)
			return inputValueClass;
		else
			return preProValueClass;
	}

	public void setPreProValueClass(Class<? extends Writable> preProValueClass) {
		this.preProValueClass = preProValueClass;
	}

	public Class<? extends Writable> getOutputKeyClass() {
		return outputKeyClass;
	}

	public void setOutputKeyClass(Class<? extends Writable> outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	public Class<? extends Writable> getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(Class<? extends Writable> outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	

	private Set<Integer> coPartitionedWithIndex = new HashSet<Integer>(5);
	
	public Set<Integer> getCoPartitionedWithIndex(){
		return this.coPartitionedWithIndex;
	}
	
	public void addCoPartitionedWithIndex(int value){
		coPartitionedWithIndex.add(value);
	}
	
	
}

