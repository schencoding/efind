package com.hp.hplc.indexoperator;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.hp.hplc.translator.MRCollector;

public abstract class ParallelIndexOperator<K1, V1, VF, K2, V2> {

	private LinkedList<SimpleIndexOperator> parallelOperators = new LinkedList<SimpleIndexOperator>();
	private Class<?> inputKeyClass;
	private Class<?> inputValueClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;

	public ParallelIndexOperator() {
		// add all operators here
	}

	public abstract void preProcess(K1 key, V1 value,
			Pair<MRCollector<Integer, Object>, VF> pair) throws IOException;

	public abstract void postProcess(K1 key, VF value,
			MRCollector<Integer, Object> indexLookupResults,
			OutputCollector<K2, V2> output) throws IOException;

	public void addParallelOperators(SimpleIndexOperator indexOp) {
		parallelOperators.add(indexOp);
	}

	public List<SimpleIndexOperator> getParallelOperators() {
		return parallelOperators;
	}

	public int getNumOfParIdxOps() {
		return parallelOperators.size();
	}

	public SimpleIndexOperator getIndexOperatpr(int i) {
		return parallelOperators.get(i);
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
