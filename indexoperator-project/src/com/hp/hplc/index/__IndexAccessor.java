package com.hp.hplc.index;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

/**
 * Base class for indexes.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public abstract class __IndexAccessor {
	private String url = null;
	private Class<? extends Writable> keyClass = null;
	private Class<? extends Writable> valueClass = null;

	public __IndexAccessor(String url) {
		this.url = url;
	}
	
	public void close() {
	}
	
	public String getUrl() {
		return (url);
	}
	
	public abstract Class<? extends Writable> getKeyClass() ;
	
	public abstract Class<? extends Writable> getValueClass() ;
	
	public void setKeyClass(Class<? extends Writable> keyClass) {
		this.keyClass = keyClass;
	}

	public void setValueClass(Class<? extends Writable> valueClass) {
		this.valueClass = valueClass;
	}
	
	public boolean isPartitioned() {
		return (false);
	}
	
	public String getPartitionClass() {
		return (null);
	}
	
	public int getNumberOfPartitions() {
		return (-1);
	}
	
	public List<String>[] getPartitionLocations() {
		return (null);
	}
	
	public abstract Vector<Writable> get(Writable key);
}

