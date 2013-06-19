package com.hp.hplc.util;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;
import java.lang.Integer;

/**
 * A cache implementing least-recently-used replacement algorithm.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-26
 */
public class MyLRUCache<K, V> {
	private Vector<K> key = null;
	private Vector<V> value = null;
	private int[] next = null;
	private int[] prev = null;
	private int head = -1;
	private int tail = -1;
	private HashMap<K, Integer> map = null;
	private int cnt = 0;
	private int capacity = 0;
	private Vector<Integer> free = null;
	private Map.Entry<K, V> removed = null;
	
	private static final float LOAD_FACTOR = 0.75f;
	
	private void remove(int idx) {
		if (prev[idx] != -1)
			next[prev[idx]] = next[idx];
		if (next[idx] != -1)
			prev[next[idx]] = prev[idx];
		if (head == idx)
			head = next[idx];
		if (tail == idx)
			tail = prev[idx];
	}
	
	private void add(int idx) {
		if (tail == -1) {
			assert head == -1;
			prev[idx] = next[idx] = -1;
			head = tail = idx;
			return;
		}
		next[tail] = idx;
		prev[idx] = tail;
		next[idx] = -1;
		tail = idx;
	}
	
	public MyLRUCache(int capacity) {
		this.key = new Vector<K>(capacity);
		while (this.key.size() < capacity)
			this.key.add(null);
		this.value = new Vector<V>(capacity);
		while (this.value.size() < capacity)
			this.value.add(null);
		this.next = new int[capacity];
		this.prev = new int[capacity];
		this.map = new HashMap<K, Integer>((int) (capacity / LOAD_FACTOR) + 1, LOAD_FACTOR);
		this.capacity = capacity;
		this.free = new Vector<Integer>(capacity);
		for (int i = 0; i < capacity; i++)
			this.free.add(i);
	}
	
	public int size() {
		assert cnt == map.size();
		return (cnt);
	}
	
	public boolean containsKey(K key) {
		if (! map.containsKey(key))
			return (false);
		
		int idx = map.get(key).intValue();
		remove(idx);
		add(idx);
		
		return (true);
	}
	
	public V get(K key) {
		Integer idx = map.get(key);
		if (idx == null)
			return (null);
		
		remove(idx.intValue());
		add(idx.intValue());
		
		return (value.get(idx.intValue()));
	}
	
	public V put(K key, V value) {
		int idx;
		V res;
		
		if (map.containsKey(key)) {
			idx = map.get(key).intValue();
			res = this.value.get(idx);
			
			this.value.set(idx, value);
			
			remove(idx);
			add(idx);
		} else if (cnt < capacity) {
			assert free.size() > 0;
			cnt++;

			idx = free.remove(free.size() - 1).intValue();
			res = null;

			map.put(key, idx);
			this.key.set(idx, key);
			this.value.set(idx, value);
			
			add(idx);
		} else {
			idx = head;
			res = null;
			
			assert idx != -1;
			map.remove(this.key.get(idx));
			removed = new SimpleEntry<K, V>(this.key.get(idx), this.value.get(idx));
			
			map.put(key, idx);
			this.key.set(idx, key);
			this.value.set(idx, value);
			
			remove(idx);
			add(idx);
		}

		return (res);
	}
	
	public void removeLeastRecentlyUsed() {
		if (cnt == 0) {
			removed = null;
			return;
		}
		
		int idx = head;
		assert idx != -1;
		map.remove(this.key.get(idx));
		remove(idx);

		cnt--;
		free.add(idx);
		
		removed = new SimpleEntry<K, V>(this.key.get(idx), this.value.get(idx));
	}

    public Map.Entry<K, V> getLastRemoved() {
    	return (this.removed);
    }
    
    public void check() {
    	int i, ptr;
    	int[] tag = null;
    	
    	assert capacity > 0;
    	assert next.length == capacity;
    	assert prev.length == capacity;
    	assert 0 <= cnt && cnt <= capacity;
    	assert 0 <= free.size() && free.size() <= capacity;
    	assert cnt + free.size() == capacity;
    	assert cnt == map.size();
    	
    	for (ptr = head, i = 1; i < cnt; i++) {
    		assert ptr != -1;
    		ptr = next[ptr];
    	}
    	assert ptr == tail;
    	assert next[ptr] == -1;
    	
    	for (ptr = tail, i = 1; i < cnt; i++) {
    		assert ptr != -1;
    		ptr = prev[ptr];
    	}
    	assert ptr == head;
    	assert prev[ptr] == -1;
    	
    	tag = new int[capacity];
    	for (i = 0; i < capacity; i++)
    		tag[i] = 1;
    	for (i = 0; i < free.size(); i++)
    		tag[free.get(i)] = 0;
    	
    	for (i = 0; i < capacity; i++)
    		if (tag[i] == 1) {
    			assert map.containsKey(this.key.get(i));
    			assert map.get(this.key.get(i)).intValue() == i;
    		}
    }
    
    public void show() {
    	int i;
    	for (i = head; i != -1; i = next[i])
    		System.out.print(i + ": (" + this.key.get(i) + ", " + this.value.get(i) + ") ");
    	System.out.println("");
    }
}

