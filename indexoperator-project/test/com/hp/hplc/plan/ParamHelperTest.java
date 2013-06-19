package com.hp.hplc.plan;

import java.util.Random;
import com.hp.hplc.plan.ParamHelper;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

public class ParamHelperTest {
	
	private static final int TEST_CNT = 10000000;
	private static final int MAX_LEN = 100;
	
	private Random rand = new Random();
	private double total = 0.0;
	private int cnt = 0;

	private String randomString() {
		StringBuffer sb = new StringBuffer();
		int len, i;
		
		len = rand.nextInt(MAX_LEN + 1);
		for (i = 0; i < len; i++)
			sb.append((char) rand.nextInt(65536));
		
		return (new String(sb));
	}

	@Test
	public void testEncodeDecode() {
		for (int i = 0; i < TEST_CNT; i++) {
			String str = randomString();
			String str2 = ParamHelper.encode(str);
			assertTrue(str.equals(ParamHelper.decode(str2)));
			if (str.length() > 0) {
				total += (double) str2.length() / str.length();
				cnt++;
			}
		}
	}

	@After
	public void after() {
		System.out.println(total / cnt);
	}
	
}
