package com.hp.hplc.translator;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Mapper4Reducer<K2,V2,K3,V3> extends MapReduceBase implements Mapper<K2, Iterator<V2>, K3, V3> {
	
	private Reducer reducer;


	@Override
	public void configure(JobConf job) {
		Class reducerClass = job.getReducerClass();
		try {
			reducer = (Reducer)reducerClass.newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}


	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void map(K2 key, Iterator<V2> values, OutputCollector<K3, V3> output,
			Reporter reporter) throws IOException {
		reducer.reduce(key, values, output, reporter);
		
	}
	
	

}
