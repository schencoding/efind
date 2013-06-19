package com.hp.hplc.indexopimpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.translator.MRCollector;

public class SimpleKeywordExtensionIndexOperator extends
		SimpleIndexOperator<Text, Text, Text, Text, List<Text>, Text, Text> {

	private Text keyWord;

	public SimpleKeywordExtensionIndexOperator(String indexAccessorClassName,
			String indexURL) {
		super(indexAccessorClassName, indexURL);
	}

	@Override
	public Class<?> getInputKeyClass() {
		return Text.class;
	}

	@Override
	public Class<?> getInputValueClass() {
		return Text.class;
	}

	@Override
	public Class<?> getOutputKeyClass() {
		return Text.class;
	}

	@Override
	public Class<?> getOutputValueClass() {
		return Text.class;
	}

	@Override
	public void preProcess(Text key, Text value, Pair<List<Text>, Text> pair)
			throws IOException {
		pair.getOne().add(key);
		pair.setTwo(value);

	}

	@Override
	public void postProcess(Text key, Text value,
			MRCollector<Text, List<Text>> indexLookupResults,
			OutputCollector output) throws IOException, InterruptedException {
		Iterator<Pair<Text, List<Text>>> it = indexLookupResults.iterator();
		while (it.hasNext()) {
			Pair<Text, List<Text>> parentPair = it.next();
			Text indexKey = parentPair.getOne();
			List<Text> indexValue = parentPair.getTwo();
			String list = "";
			list += " " + indexKey.toString() + ":" + indexValue.toString();
			output.collect(key, new Text(list));

		}

	}

}
