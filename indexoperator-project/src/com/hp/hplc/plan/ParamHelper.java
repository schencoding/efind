package com.hp.hplc.plan;

public class ParamHelper {
	private static char[] code2hex =
		{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
	private static int[] hex2code = new int[256];
	
	static {
		char c;
		for (c = '0'; c <= '9'; c++)
			hex2code[((int) c) & 0xFF] = c - '0';
		for (c = 'A'; c <= 'F'; c++)
			hex2code[((int) c) & 0xFF] = 10 + (c - 'A');
	}
	
	public static String encode(String str) {
		StringBuffer sb = new StringBuffer();
		int i;

		for (i = 0; i < str.length(); i++) {
			int code = (int) str.charAt(i);
			sb.append(code2hex[(code >>> 12) & 0xF]);
			sb.append(code2hex[(code >>> 8) & 0xF]);
			sb.append(code2hex[(code >>> 4) & 0xF]);
			sb.append(code2hex[code & 0xF]);
		}

		return (new String(sb));
	}
	
	public static String decode(String str) {
		StringBuffer sb = new StringBuffer();
		int i;
		
		assert(str.length() % 4 == 0);
		for (i = 0; i < str.length(); ) {
			int code = 0;
			code = hex2code[((int) str.charAt(i++)) & 0xFF];
			code = (code << 4) | hex2code[((int) str.charAt(i++)) & 0xFF];
			code = (code << 4) | hex2code[((int) str.charAt(i++)) & 0xFF];
			code = (code << 4) | hex2code[((int) str.charAt(i++)) & 0xFF];
			sb.append((char) code);
		}
		
		return (new String(sb));
	}
}
