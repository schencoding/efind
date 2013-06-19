package com.hp.hplc.translator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.Pair;

public class Reducer4IndexOperator extends MapReduceBase
		implements Reducer<Writable, Writable, Writable, Writable> {

	private IndexOperator indexOperator;
	

	@Override
	public void configure(JobConf conf) {
		String indexOperatorClassName = conf.get("indexOperator.className");
		String indexOperatorURL = conf.get("indexOperator.url");
		String indexOperatorAccessorClassName = conf.get("indexOperator.accerrosClassName");

		if (indexOperatorClassName != null) {
			try {
				Class c = Class.forName(indexOperatorClassName);
				if (c != null) {
					Constructor constructor = c.getConstructor(String.class);
					if(constructor != null){
						this.indexOperator = (IndexOperator)(constructor.newInstance(indexOperatorAccessorClassName, indexOperatorURL));
					}
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}



	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void reduce(Writable key, Iterator<Writable> values,
			OutputCollector<Writable, Writable> output, Reporter reporter)
			throws IOException {
		while (values.hasNext()) {
			Writable value = values.next();
			Pair<MRCollector<Integer, Object>, Object> pair = new Pair<MRCollector<Integer, Object>, Object>();
			pair.setOne(new MRCollector<Integer, Object>());
			System.out.println("Input key: " + key + " Input value: " + value);

			indexOperator.preProcess(key, value, pair);

			MRCollector<Pair<Integer, Object>, Object> indexResultsCollector = new MRCollector<Pair<Integer, Object>, Object>();

			Object valueAfterPrePro = pair.getTwo();

			MRCollector<Integer, Object> indexKeyCollector = pair.getOne();
			Iterator<Pair<Integer, Object>> it = indexKeyCollector.iterator();
			while (it.hasNext()) {
				Pair<Integer, Object> indexPair = it.next();
				Integer indexID = indexPair.getOne();
				Object indexKey = indexPair.getTwo();

				//
				// Add cache here

				//
				Object indexValue = indexOperator.get(indexID, indexKey);

				indexResultsCollector.collect(indexPair, indexValue);
			}

			try {
				indexOperator.postProcess(key, valueAfterPrePro,
						indexResultsCollector, output);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		
		
	}

}
