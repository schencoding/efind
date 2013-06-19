package com.hp.hplc.util;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;

import org.junit.Test;

import com.hp.hplc.util.LRUCache;
import com.hp.hplc.util.MyLRUCache;

public class LRUCacheTest {
	private LRUCache<Object, Object> cache = null;
	private MyLRUCache<Object, Object> cache2 = null;
	private Random rand = new Random();
	
	private static final int TEST_CNT = 1000000;
	private static final int MAX_CAPACITY = 10;
	private static final int MAX_OPERATIONS = 200;
	
	private int[] _key = new int[MAX_CAPACITY];
	private int[] _value = new int[MAX_CAPACITY];
	private int _capacity = 0, _cnt = 0;
	private int _last_removed_key = 0, _last_removed_value = 0;
	
	private boolean touch(int key) {
		int i, value;
		
		for (i = 0; i < _cnt; i++)
			if (_key[i] == key)
				break;
		
		if (i == _cnt)
			return (false);
		
		value = _value[i];
		for (i++; i < _cnt; i++) {
			_key[i - 1] = _key[i];
			_value[i - 1] = _value[i];
		}
		_key[_cnt - 1] = key;
		_value[_cnt - 1] = value;
		
		return (true);
	}
	
	private void put(int key, int value) {
		int i;
		
		for (i = 0; i < _cnt; i++)
			if (_key[i] == key)
				break;
		if (i < _cnt) {
			_value[i] = value;
			touch(key);
			return;
		}
		
		if (_cnt == _capacity) {
			_last_removed_key = _key[0];
			_last_removed_value = _value[0];
			for (i = 1; i < _cnt; i++) {
				_key[i - 1] = _key[i];
				_value[i - 1] = _value[i];
			}
			_cnt--;
		}
		_key[_cnt] = key;
		_value[_cnt] = value;
		_cnt++;
	}
	
	private void verify() {
		int i;
		Integer value;
		
		assertTrue(_cnt == cache.size());
		assertTrue(_cnt == cache2.size());
		for (i = 0; i < _cnt; i++) {
			value = (Integer) cache.get(_key[i]);
			assertTrue(value != null);
			assertTrue(value.intValue() == _value[i]);
			
			value = (Integer) cache2.get(_key[i]);
			assertTrue(value != null);
			assertTrue(value.intValue() == _value[i]);
		}
	}

	@Test
	public void testLRUCache() {
		int i, j, key, value = 0;
		Map.Entry<Object, Object> removed = null;
		
		for (i = 0; i < TEST_CNT; i++) {
			int capacity = rand.nextInt(MAX_CAPACITY) + 1;
			int operations = rand.nextInt(MAX_OPERATIONS) + 1;
			
			_capacity = capacity;
			_cnt = 0;
			cache = new LRUCache<Object, Object>(capacity);
			cache2 = new MyLRUCache<Object, Object>(capacity);
			
			for (j = 0; j < operations; j++) {
				key = rand.nextInt(4 * capacity);
				value = rand.nextInt();
				
				assertTrue(touch(key) == cache.containsKey(key));
				assertTrue(touch(key) == cache2.containsKey(key));

				put(key, value);
					
				cache.put(key, value);
				removed = cache.getLastRemoved();
				if (removed != null) {
					assertTrue(((Integer) removed.getKey()).intValue() == _last_removed_key);
					assertTrue(((Integer) removed.getValue()).intValue() == _last_removed_value);
				}
					
				cache2.put(key, value);
				removed = cache2.getLastRemoved();
				if (removed != null) {
					assertTrue(((Integer) removed.getKey()).intValue() == _last_removed_key);
					assertTrue(((Integer) removed.getValue()).intValue() == _last_removed_value);
				}
				
				assertTrue(cache.getLastRemoved() != null && cache2.getLastRemoved() != null ||
					cache.getLastRemoved() == null && cache2.getLastRemoved() == null);
			}
			
			cache2.check();
			verify();
		}
	}
}
