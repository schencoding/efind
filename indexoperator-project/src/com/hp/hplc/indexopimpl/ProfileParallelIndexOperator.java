package com.hp.hplc.indexopimpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.indexoperator.ParallelIndexOperator;

import com.hp.hplc.translator.MRCollector;


public class ProfileParallelIndexOperator extends
		ParallelIndexOperator<Text, Text, Text, Text, Text> {
	
	public ProfileParallelIndexOperator(){
		SimpleIndexOperator profileIdxOp1 = new SimpleProfileIndexOperator(
				"com.hp.hplc.index.HashIndexAccessor",
				"Hash://localhost/Profile");
		this.addParallelOperators(profileIdxOp1);
		
		SimpleIndexOperator profileIdxOp2 = new SimpleProfileIndexOperator(
				"com.hp.hplc.index.HashIndexAccessor",
				"Hash://localhost/Profile");
		this.addParallelOperators(profileIdxOp2);
	}


	@Override
	public Class<?> getOutputKeyClass() {
		// TODO Auto-generated method stub
		return Text.class;
	}

	@Override
	public Class<?> getOutputValueClass() {
		// TODO Auto-generated method stub
		return Text.class;
	}

	@Override
	public void preProcess(Text key, Text value,
			Pair<MRCollector<Integer, Object>, Text> pair) throws IOException {
		int numOfIdxOp = this.getParallelOperators().size();
		for(int i=0; i<numOfIdxOp; i++){
			pair.getOne().collect(i, key);
		}
		pair.setTwo(value);
		
	}

	@Override
	public void postProcess(Text key, Text value,
			MRCollector<Integer, Object> indexLookupResults,
			OutputCollector<Text, Text> output) throws IOException {
		int numOfIdxOp = this.getParallelOperators().size();
		
		Iterator<Pair<Integer, Object>> it = indexLookupResults.iterator();
		while(it.hasNext()){
			Pair<Integer, Object> parentPair = it.next();
			String str = "";
			str += "" + parentPair.getOne() + "_" + parentPair.getTwo();
			Text text = new Text(str);
			output.collect(key, text);
		}
		
	}
	
	
	
	

}
