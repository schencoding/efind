package com.hp.hplc.indexoperator.util;

import java.util.Random;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

import static org.junit.Assert.*;

import org.junit.Test;

public class IndexInputTest {
	private static final int TEST_CNT = 500000;
	private static final int MAX_INDEX_CNT = 10;
	private static final int MAX_KEY_CNT = 100;
	private Random rand = new Random();
	
	@Test
	public void testIndexInput() {
		int t, i, j, cnt;
		Class<? extends Writable>[] cls = null;
		IndexInput keys = null;
		
		for (t = 0; t < TEST_CNT; t++) {
			cnt = rand.nextInt(MAX_INDEX_CNT) + 1;
			cls = new Class [cnt];
			keys = new IndexInput(cnt, cls);
			for (i = 0; i < cnt; i++) {
				cls[i] = IntWritable.class;
				int key_cnt = rand.nextInt(MAX_KEY_CNT + 1);
				for (j = 0; j < key_cnt; j++)
					keys.put(i, new IntWritable(rand.nextInt()));
			}

			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				keys.write(dos);
				
				byte[] buf = baos.toByteArray();
				
				ByteArrayInputStream bais = new ByteArrayInputStream(buf);
				DataInputStream dis = new DataInputStream(bais);
				IndexInput got = IndexInput.read(dis);
				
				assertTrue(keys.equals(got));
			} catch(Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
