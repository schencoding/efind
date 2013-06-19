package com.hp.hplc.mrimpl1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FrequencyReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, IntWritable> {

//	private IntWritable result = new IntWritable();

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		IntWritable result = new IntWritable();
		int sum = 0;
		while (values.hasNext()) {
			values.next();
			sum += 1;
		}
		result.set(sum);
		output.collect(key, result);

	}

}
