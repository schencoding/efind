package com.hp.hplc.translator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.indexoperator.SimpleIndexOperator;

public class Mapper4SimpleIndexOperator extends MapReduceBase implements
		Mapper<Writable, Writable, Writable, Writable> {

	private SimpleIndexOperator indexOperator;

	@Override
	public void configure(JobConf conf) {
		String indexOperatorClassName = conf.get("indexOperator.className");
		String indexOperatorURL = conf.get("indexOperator.url");
		String indexOperatorAccessorClassName = conf
				.get("indexOperator.accerrosClassName");

		if (indexOperatorClassName != null) {
			try {
				Class c = Class.forName(indexOperatorClassName);
				if (c != null) {
					Constructor constructor = c.getConstructor(String.class,
							String.class);
					if (constructor != null) {
						this.indexOperator = (SimpleIndexOperator) (constructor
								.newInstance(indexOperatorAccessorClassName,
										indexOperatorURL));
					}
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
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
	public void map(Writable key, Writable value,
			OutputCollector<Writable, Writable> output, Reporter reporter)
			throws IOException {
		Pair<List<Object>, Object> pair = new Pair<List<Object>, Object>();
		pair.setOne(new LinkedList<Object>());

		indexOperator.preProcess(key, value, pair);

		MRCollector<Object, Object> indexResultsCollector = new MRCollector<Object, Object>();

		Object valueAfterPrePro = pair.getTwo();

		List<Object> keyList = pair.getOne();
		Iterator<Object> it = keyList.iterator();
		while (it.hasNext()) {
			Object indexKey = it.next();

			//
			// Add cache here

			//
			Object indexValue = indexOperator.get(indexKey);

			indexResultsCollector.collect(indexKey, indexValue);
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
