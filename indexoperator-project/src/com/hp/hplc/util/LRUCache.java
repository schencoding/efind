package com.hp.hplc.util;

import java.util.Map;
import java.util.LinkedHashMap;

/**
 * A cache implementing least-recently-used replacement algorithm.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 6247721463076148177L;
	private int capacity = 0;
	private Map.Entry<K, V> removed = null;
	
	private static final float LOAD_FACTOR = 0.75f;
	
	public LRUCache(int capacity) {
		super((int) (capacity / LOAD_FACTOR) + 1, LOAD_FACTOR, true);
		this.capacity = capacity;
	}

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    	if (size() > this.capacity) {
    		removed = eldest;
    		return (true);
    	}
    	removed = null;
    	return (false);
    }
    
    public Map.Entry<K, V> getLastRemoved() {
    	return (this.removed);
    }
}

