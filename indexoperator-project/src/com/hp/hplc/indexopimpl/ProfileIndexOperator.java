package com.hp.hplc.indexopimpl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;
import com.hp.hplc.translator.MRCollector;

public class ProfileIndexOperator extends
		__IndexOperator {

	public ProfileIndexOperator() {
		super();
	}

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		
		
		Text text = (Text)key;
		/*if(text.toString().compareToIgnoreCase("User3") > 0){
			return;
		}*/
		keys.put(0, key);
		keys.put(0, key);
		keys.put(0, key);
		keys.put(0, key);
		keys.put(0, key);
		
		keys.put(1, key);
		
		Text valueTxt = (Text) value;
		valueTxt.set("12345678901234567890");
		
		return (true);
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys, IndexOutput values,
			OutputCollector<Writable, Writable> output) {
		int numOfKeys = keys.size(0);
		for(int i=0; i<numOfKeys; i++){
			Iterator<Writable> it = values.iterator(0, i);
			while (it.hasNext()) {
				Writable v = it.next();
				try {
					output.collect(key, v);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		numOfKeys = keys.size(1);
		for(int i=0; i<numOfKeys; i++){
			Iterator<Writable> it1 = values.iterator(1, i);
			while (it1.hasNext()) {
				Writable v = it1.next();
				try {
					output.collect(key, v);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}


	

	/*@Override
	public void preProcess(Text key, Text value,
			Pair<MRCollector<Integer, Object>, Text> valuesAfterPrePro)
			throws IOException {
		valuesAfterPrePro.getOne().collect(0, key);
		valuesAfterPrePro.setTwo(value);

	}

	@Override
	public void postProcess(Text key, Text value,
			MRCollector<Pair<Integer, Object>, Object> indexLookupResults,
			OutputCollector output) throws IOException, InterruptedException {
		output.collect(key, value);

		Iterator<Pair<Pair<Integer, Object>, Object>> it = indexLookupResults
				.iterator();
		while (it.hasNext()) {
			Pair<Pair<Integer, Object>, Object> parentPair = it.next();
			Pair<Integer, Object> indexKey = parentPair.getOne();
			Object indexValue = parentPair.getTwo();
			String list = "";
			list += " " + indexKey.getTwo().toString() + ":"
					+ indexValue.toString();
			output.collect(key, new Text(list));

		}
	}*/

}
