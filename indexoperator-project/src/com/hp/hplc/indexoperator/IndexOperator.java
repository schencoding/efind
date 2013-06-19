package com.hp.hplc.indexoperator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.translator.MRCollector;

public abstract class IndexOperator<K1, V1, VF, K2, V2> {

	private LinkedList<__IndexAccessor> indexAccessors = new LinkedList<__IndexAccessor>();
	private Class<?> inputKeyClass;
	private Class<?> inputValueClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;

	public IndexOperator() {
	}

	public IndexOperator(List<Pair<String, String>> accessorDescriptors) {
		Iterator<Pair<String, String>> it = accessorDescriptors.iterator();
		while (it.hasNext()) {
			Pair<String, String> pair = it.next();
			String indexAccessorClassName = pair.getOne();
			String indexURL = pair.getTwo();
			Class cls;
			try {
				cls = Class.forName(indexAccessorClassName);
				if (cls != null) {
					Constructor constructor = cls.getConstructor(String.class);
					if (constructor != null) {
						__IndexAccessor indexAccessor = (__IndexAccessor) constructor
								.newInstance(indexURL);
						indexAccessors.add(indexAccessor);
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

	public abstract void preProcess(K1 key, V1 value,
			Pair<MRCollector<Integer, Object>, VF> valuesAfterPrePro) throws IOException;

	public abstract void postProcess(K1 key, VF value,
			MRCollector<Pair<Integer, Object>, Object> indexLookupResults,
			OutputCollector output) throws IOException, InterruptedException;

	public Object get(int i, Object key) {
		return indexAccessors.get(i).get((Writable) key);
	}

	public Class<?> getInputKeyClass() {
		return inputKeyClass;
	}

	public void setInputKeyClass(Class<?> inputKeyClass) {
		this.inputKeyClass = inputKeyClass;
	}

	public Class<?> getInputValueClass() {
		return inputValueClass;
	}

	public void setInputValueClass(Class<?> inputValueClass) {
		this.inputValueClass = inputValueClass;
	}

	public Class<?> getOutputKeyClass() {
		return outputKeyClass;
	}

	public void setOutputKeyClass(Class<?> outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	public Class<?> getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(Class<?> outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

}
