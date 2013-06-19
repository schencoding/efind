package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.K2V2Writable;

import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;
import com.hp.hplc.util.Pair;

/**
 * Descriptor of an index preprocess task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-11
 */
public class IndexPreTaskDescriptor extends TaskDescriptor implements Serializable {
	private static final long serialVersionUID = -6225072508941982702L;
	private Class<? extends __IndexOperator> task = null;
	private transient __IndexOperator obj = null;
	private transient Class<? extends Writable>[] keyClasses = null;
	
	private Vector<String> names = null;
	private Vector<String> urls = null;
	
	private int coPartitionIndex = -1;
	
	public IndexPreTaskDescriptor(__IndexOperator task, TaskType type, int id)
		throws InvalidPlanException {
		super(type, id);
		this.task = task.getClass();
		this.names = task.getNames();
		this.urls = task.getUrls();
	}
	
	public IndexPreTaskDescriptor(__IndexOperator task, TaskType type)
		throws InvalidPlanException {
		super(type);
		this.task = task.getClass();
		this.names = task.getNames();
		this.urls = task.getUrls();
	}
	
	public void close() {
		if (obj != null)
			obj.close();
	}
	
	public void setCoPartitionIndex(int index) {
		this.coPartitionIndex = index;
	}
	
	public String toString() {
		return ("IndexPreTask");
	}
	
	public void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.MAP)
			throw new InvalidTaskRequestException();
		
		if (obj == null) {
			obj = task.newInstance();
			keyClasses = new Class [names.size()];
			for (int i = 0; i < names.size(); i++)
				obj.addIndex(names.get(i), urls.get(i));
			Vector<__IndexAccessor> accessors = obj.getInternal();
			for (int i = 0; i < names.size(); i++)
				keyClasses[i] = accessors.get(i).getKeyClass();
		}
		
		try {
			if (count) {
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_INPUT_RECORDS), 1);
				if (key instanceof Text)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_KEY_BYTES), ((Text) key).getLength());
				else if (key instanceof BytesWritable)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_KEY_BYTES), ((BytesWritable) key).getLength());
				if (value instanceof Text)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_VALUE_BYTES), ((Text) value).getLength());
				else if (value instanceof BytesWritable)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_VALUE_BYTES), ((BytesWritable) value).getLength());
			}
			
			/*for(Class cls : keyClasses){
				System.out.println("IndexPreTaskDescriptor->keyClass: " + cls.getName());
			}*/
			
			IndexInput keys = new IndexInput(obj.size(), keyClasses);
			if (! obj.preprocess(key, value, keys))
				return;
			
			//System.out.println("IndexPreTaskDescriptor.getInputKeyClass() = " + getInputKeyClass());
			//System.out.println("IndexPreTaskDescriptor.getInputValueClass() = " + getInputValueClass());
			
			K2V2Writable k2v2 = new K2V2Writable(key, value, keys, K2V2Writable.EMPTY_OUTPUT,
				getInputKeyClass(), getInputValueClass());

			if (coPartitionIndex == -1)
				output.collect(new Text("0"), k2v2);
			else {
				Vector<Writable> key_list = keys.getInternal()[coPartitionIndex];
				if (key_list.size() > 0){
					output.collect(key_list.get(0), k2v2);
					//System.out.println("IndexPreTaskDescriptor.key_list.get(0) = " + key_list.get(0) + " type "
					//		+ key_list.get(0).getClass().getName());
				}
				else
					output.collect(new IntWritable(0), k2v2);
			}
			
			if (count) {
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_RECORDS), 1);
				if (key instanceof Text)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_KEY_BYTES), ((Text) key).getLength());
				else if (key instanceof BytesWritable)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_KEY_BYTES), ((BytesWritable) key).getLength());
				if (value instanceof Text)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_VALUE_BYTES), ((Text) value).getLength());
				else if (value instanceof BytesWritable)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_VALUE_BYTES), ((BytesWritable) value).getLength());
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void exec(Writable key, Iterator<Writable> values,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.REDUCE)
			throw new InvalidTaskRequestException();

		try {
			/*
			 * We only count original key/value pairs, not intermediate output.
			 * Therefore, it is safe to pass the count flag here.
			 */
			while (values.hasNext())
				exec(key, values.next(), output, reporter, count);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void print(){
		String str = "";
		str += this.task.toString();
		str += "\t" + "Pre-Process " + this.getType() + "\n";
		Iterator<Pair<String, String>> it = this.getProperties().iterator();
		while(it.hasNext()){
			Pair<String, String> pair = it.next();
			str += "\t" + pair.first + " = " + pair.second + "\n";
		}
		System.out.print(str);
	}
	

	public __IndexOperator getTask() throws InstantiationException, IllegalAccessException{
		if (obj == null) {
			obj = task.newInstance();
			keyClasses = new Class [names.size()];
			for (int i = 0; i < names.size(); i++)
				obj.addIndex(names.get(i), urls.get(i));
		}
		return this.obj;
	}
}
