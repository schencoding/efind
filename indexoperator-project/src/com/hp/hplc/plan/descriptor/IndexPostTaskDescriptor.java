package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;
import com.hp.hplc.indexoperator.util.K2V2Writable;

import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;
import com.hp.hplc.util.Pair;
import com.hp.hplc.util.RetrievableOutputCollector;

/**
 * Descriptor of a index postprocess task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-12
 */
public class IndexPostTaskDescriptor extends TaskDescriptor implements Serializable {
	private static final long serialVersionUID = -8569294856145617380L;
	private Class<? extends __IndexOperator> task = null;
	private transient __IndexOperator obj = null;
	
	private Vector<String> names = null;
	private Vector<String> urls = null;
	
	public IndexPostTaskDescriptor(__IndexOperator task, TaskType type, int id)
		throws InvalidPlanException {
		super(type, id);
		this.task = task.getClass();
		this.names = task.getNames();
		this.urls = task.getUrls();
	}
	
	public IndexPostTaskDescriptor(__IndexOperator task, TaskType type)
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
	
	public String toString() {
		return ("IndexPostTask");
	}
	
	public void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.MAP)
			throw new InvalidTaskRequestException();
		
		if (obj == null) {
			obj = task.newInstance();
			for (int i = 0; i < names.size(); i++)
				obj.addIndex(names.get(i), urls.get(i));
		}
		
		try {
			Writable key0 = null;
			Writable value0 = null;
			IndexInput keys = null;
			IndexOutput values = null;
			K2V2Writable k2v2 = (K2V2Writable) value;
			
			key0 = k2v2.getKey();
			value0 = k2v2.getValue();
			keys = k2v2.getKeys();
			values = k2v2.getValues();

			obj.postprocess(key0, value0, keys, values, output);
			
			if (count) {
				RetrievableOutputCollector<Writable, Writable> out = (RetrievableOutputCollector<Writable, Writable>) output;
				Iterator<Pair<Writable, Writable> > itr = out.iterator();
				long task_output_records = 0;
				long task_output_key_bytes = 0;
				long task_output_value_bytes = 0;
				while (itr.hasNext()) {
					Pair<Writable, Writable> pair = itr.next();
					task_output_records++;
					if (pair.first instanceof Text)
						task_output_key_bytes += ((Text) pair.first).getLength();
					else if (pair.first instanceof BytesWritable)
						task_output_key_bytes += ((BytesWritable) pair.first).getLength();
					if (pair.second instanceof Text)
						task_output_value_bytes += ((Text) pair.second).getLength();
					else if (pair.second instanceof BytesWritable)
						task_output_value_bytes += ((BytesWritable) pair.second).getLength();
				}
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_RECORDS), task_output_records);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_KEY_BYTES), task_output_key_bytes);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_VALUE_BYTES), task_output_value_bytes);
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
			while (values.hasNext())
				exec(key, values.next(), output, reporter, false);
			
			if (count) {
				RetrievableOutputCollector<Writable, Writable> out = (RetrievableOutputCollector<Writable, Writable>) output;
				Iterator<Pair<Writable, Writable> > itr = out.iterator();
				long task_output_records = 0;
				long task_output_key_bytes = 0;
				long task_output_value_bytes = 0;
				while (itr.hasNext()) {
					Pair<Writable, Writable> pair = itr.next();
					task_output_records++;
					if (pair.first instanceof Text)
						task_output_key_bytes += ((Text) pair.first).getLength();
					else if (pair.first instanceof BytesWritable)
						task_output_key_bytes += ((BytesWritable) pair.first).getLength();
					if (pair.second instanceof Text)
						task_output_value_bytes += ((Text) pair.second).getLength();
					else if (pair.second instanceof BytesWritable)
						task_output_value_bytes += ((BytesWritable) pair.second).getLength();
				}
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_RECORDS), task_output_records);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_KEY_BYTES), task_output_key_bytes);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_OUTPUT_VALUE_BYTES), task_output_value_bytes);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void print(){
		String str = "";
		str += this.task.toString();
		str += "\t" + "Post-Process " + this.getType() + "\n";
		Iterator<Pair<String, String>> it = this.getProperties().iterator();
		while(it.hasNext()){
			Pair<String, String> pair = it.next();
			str += "\t" + pair.first + " = " + pair.second + "\n";
		}
		System.out.print(str);
	}

	public __IndexOperator getTask() {
		return this.obj;
	}
}
