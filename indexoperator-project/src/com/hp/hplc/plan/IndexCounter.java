package com.hp.hplc.plan;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class IndexCounter {
	public static final String DELIMITER = "$";

	public static final String GROUP = "HPLC_INDEX_OPERATOR_PROJECT";

	public static final String TASK_INPUT_RECORDS = "TASK_INPUT_RECORDS";
	public static final String TASK_INPUT_KEY_BYTES = "TASK_INPUT_KEY_BYTES";
	public static final String TASK_INPUT_VALUE_BYTES = "TASK_INPUT_VALUE_BYTES";
	public static final String TASK_OUTPUT_RECORDS = "TASK_OUTPUT_RECORDS";
	public static final String TASK_OUTPUT_KEY_BYTES = "TASK_OUTPUT_KEY_BYTES";
	public static final String TASK_OUTPUT_VALUE_BYTES = "TASK_OUTPUT_VALUES_BYTES";
	
	public static final String INDEX_INPUT_KEYS = "INDEX_INPUT_KEYS";
	public static final String INDEX_INPUT_BYTES = "INDEX_INPUT_BYTES";
	public static final String INDEX_OUTPUT_VALUES = "INDEX_OUTPUT_VALUES";
	public static final String INDEX_OUTPUT_BYTES = "INDEX_OUTPUT_BYTES";
	public static final String INDEX_CACHE_HIT = "INDEX_CACHE_HIT";
	public static final String INDEX_CACHE_MISS = "INDEX_CACHE_MISS";
	public static final String INDEX_LOOKUP_TIME = "INDEX_LOOKUP_TIME";
	
	public static String get(int taskID, String counter) {
		return (String.valueOf(taskID) + DELIMITER + counter);
	}
	
	public static String get(int taskID, int indexID, String counter) {
		return (String.valueOf(taskID) + DELIMITER +
			String.valueOf(indexID) + DELIMITER + counter);
	}

	public static int getBytes(int cnt, int bytes, Class<? extends Writable> cls) {
		if (cls.equals(Text.class) || cls.equals(BytesWritable.class))
			return (bytes);
		if (cls.equals(BooleanWritable.class))
			return (cnt * 1);
		if (cls.equals(ByteWritable.class))
			return (cnt * 1);
		if (cls.equals(IntWritable.class))
			return (cnt * 4);
		if (cls.equals(LongWritable.class))
			return (cnt * 8);
		if (cls.equals(FloatWritable.class))
			return (cnt * 4);
		if (cls.equals(DoubleWritable.class))
			return (cnt * 8);
		return (-1);
	}
}

