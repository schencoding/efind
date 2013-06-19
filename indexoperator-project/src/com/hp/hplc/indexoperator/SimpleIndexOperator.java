package com.hp.hplc.indexoperator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.translator.MRCollector;

public abstract class SimpleIndexOperator<K1, V1, VF, IK, IV, K2, V2> {

	private __IndexAccessor indexAccessor;
	private String indexURL;
	private Class<?> inputKeyClass;
	private Class<?> inputValueClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;

	public SimpleIndexOperator() {
	}

	public SimpleIndexOperator(String indexAccessorClassName, String indexURL) {
		Class cls;
		try {
			cls = Class.forName(indexAccessorClassName);
			if (cls != null) {
				Constructor constructor = cls.getConstructor(String.class);
				if (constructor != null) {
					indexAccessor = (__IndexAccessor) constructor
							.newInstance(indexURL);
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

		this.indexURL = indexURL;
	}

	public abstract void preProcess(K1 key, V1 value, Pair<List<IK>, VF> pair)
			throws IOException;

	public abstract void postProcess(K1 key, VF value,
			MRCollector<IK, IV> indexLookupResults, OutputCollector output)
			throws IOException, InterruptedException;

	public __IndexAccessor getIndexAccessor() {
		return indexAccessor;
	}

	public IV get(IK key) {
		return (IV) indexAccessor.get((Writable) key);
	}

	public String getIndexURL() {
		return indexURL;
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
