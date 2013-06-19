package com.hp.hplc.translator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.Pair;

public class MRCollector<K,V> implements OutputCollector<K,V>{
	
	private LinkedList<Pair<K,V>> list = new LinkedList<Pair<K,V>>();

	@Override
	public void collect(K key, V value) throws IOException {
		list.add(new Pair(key,value));			
	}

	@Override
	public String toString() {
		String str = "";
		Iterator<Pair<K, V>> it = list.iterator();
		while(it.hasNext()){
			Pair obj = it.next();
			str += obj.getTwo() + " ";
		}
		return str;
	}
	
	public Iterator<Pair<K,V>> iterator(){
		return list.iterator();
	}
	
}