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

public class KeywordExtensionIndexOperator extends IndexOperator<Text, Text, Text, List<Text>, Text>{

	private Text keyWord;

	public KeywordExtensionIndexOperator(List<Pair<String, String>> indexList) {
		super(indexList);
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
	public void preProcess(Text key, Text value,
			Pair<MRCollector<Integer, Object>, Text> valuesAfterPrePro)
			throws IOException {
		valuesAfterPrePro.getOne().collect(0, key);
		valuesAfterPrePro.setTwo(valuesAfterPrePro.getTwo());		
	}


	@Override
	public void postProcess(Text key, Text value,
			MRCollector<Pair<Integer, Object>, Object> indexLookupResults,
			OutputCollector output) throws IOException, InterruptedException {
		output.collect(key, value);
		
		Iterator<Pair<Pair<Integer, Object>, Object>> it =  indexLookupResults.iterator();
		while(it.hasNext()){
			Pair<Pair<Integer, Object>, Object> parentPair = it.next();
			Pair<Integer, Object> indexKey = parentPair.getOne();
			Object indexValue = parentPair.getTwo();
			String list = "";
			list += " " + indexKey.getTwo().toString()+":"+indexValue.toString();
			output.collect(key, new Text(list));	
			
		}		
	}



	
	
	
	

	

}
