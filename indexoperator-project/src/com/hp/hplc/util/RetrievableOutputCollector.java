package com.hp.hplc.util;

import java.lang.Iterable;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.mapred.OutputCollector;

/**
 * Output collector from which we can get out data back.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-23
 */
public class RetrievableOutputCollector<K, V>
	implements OutputCollector<K, V>, Iterable<Pair<K, V> > {
	private LinkedList<Pair<K, V> > list = null;
	
	public RetrievableOutputCollector() {
		list = new LinkedList<Pair<K, V> >();
	}
	
	public void collect(K key, V value) {
		list.add(new Pair<K, V>(key, value));
	}
	
	public Iterator<Pair<K, V> > iterator() {
		return (list.iterator());
	}
}
