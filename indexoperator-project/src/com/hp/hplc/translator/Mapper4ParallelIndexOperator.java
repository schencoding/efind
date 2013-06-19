package com.hp.hplc.translator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.indexoperator.ParallelIndexOperator;

public class Mapper4ParallelIndexOperator extends MapReduceBase 
implements Mapper<Writable, Writable, Writable, Writable>{
	
	ParallelIndexOperator parIdxOp;

	@Override
	public void map(Writable key, Writable value,
			OutputCollector<Writable, Writable> output, Reporter reporter)
			throws IOException {
		
		Pair<MRCollector<Integer, Object>, Object> pair = new Pair<MRCollector<Integer, Object>, Object>();
		pair.setOne(new MRCollector<Integer, Object>());
		
		parIdxOp.preProcess(key, value, pair);
		
		MRCollector<Integer, Object> eachIndexPostProResults = new MRCollector<Integer, Object>();
		
		//Map<Integer, Object> idxPostProcessValues = new HashMap<Integer, Object>();
		List<MRCollector> collectorList = new ArrayList<MRCollector>();
		for(int i =0; i<parIdxOp.getNumOfParIdxOps();i++){
			collectorList.add(new MRCollector());
		}
		
		
		Iterator<Pair<Integer, Object>> it = pair.getOne().iterator();
		while(it.hasNext()){
			Pair<Integer, Object> indexKeyPair = it.next();
			Integer indexID = indexKeyPair.getOne();
			Object indexKey = indexKeyPair.getTwo();
			
			SimpleIndexOperator idxOp = parIdxOp.getIndexOperatpr(indexID);
			Object indexLookupResult = idxOp.get(indexKey);
			
			collectorList.get(indexID).collect(indexKey, indexLookupResult);			
		}
		
		for(int i =0; i<parIdxOp.getNumOfParIdxOps();i++){
			SimpleIndexOperator idxOp = parIdxOp.getIndexOperatpr(i);			
			MRCollector myOutput = new MRCollector();
			try {
				idxOp.postProcess(key, pair.getTwo(), collectorList.get(i), myOutput);
				eachIndexPostProResults.collect(i, myOutput);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}				
		}
		
		parIdxOp.postProcess(key, pair.getTwo(), eachIndexPostProResults, output);
		
	}
	
	@Override
	public void configure(JobConf conf) {
		String parIdxOpClassName = conf.get("indexOperator.className");

		if (parIdxOpClassName != null) {
			try {
				Class c = Class.forName(parIdxOpClassName);
				if (c != null) {
					Constructor constructor = c.getConstructor();
					if(constructor != null){
						this.parIdxOp = (ParallelIndexOperator)(constructor.newInstance());
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
	
	

}
