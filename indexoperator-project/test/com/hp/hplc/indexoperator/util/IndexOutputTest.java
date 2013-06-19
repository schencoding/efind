package com.hp.hplc.indexoperator.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class IndexOutputTest {
	private static final int TEST_CNT = 10000;
	private static final int MAX_INDEX_CNT = 10;
	private static final int MAX_KEY_CNT = 100;
	private static final int MAX_VALUE_CNT = 100;
	private Random rand = new Random();

	@Test
	public void testIndexOutput() {
		int t, i, j, k, cnt;
		Class<? extends Writable>[] cls = null;
		IndexInput keys = null;
		IndexOutput values = null;
		
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
		
			values = new IndexOutput(keys, cls);
			
			Vector<Writable>[] internal = keys.getInternal();
			for (i = 0; i < internal.length; i++)
				for (j = 0; j < internal[i].size(); j++) {
					cnt = rand.nextInt(MAX_VALUE_CNT + 1);
					for (k = 0; k < cnt; k++)
						values.put(i, j, new IntWritable(rand.nextInt()));
				}
	
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				values.write(dos);
				
				byte[] buf = baos.toByteArray();
				
				ByteArrayInputStream bais = new ByteArrayInputStream(buf);
				DataInputStream dis = new DataInputStream(bais);
				IndexOutput got = IndexOutput.read(dis);
				
				assertTrue(values.equals(got));
			} catch(Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
