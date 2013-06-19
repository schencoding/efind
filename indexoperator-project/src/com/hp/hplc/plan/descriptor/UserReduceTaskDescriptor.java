package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;
import com.hp.hplc.util.Pair;
import com.hp.hplc.util.RetrievableOutputCollector;

/**
 * Descriptor of a user reduce task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-12
 */
public class UserReduceTaskDescriptor extends TaskDescriptor implements Serializable {
	private static final long serialVersionUID = -8208750570217511366L;
	protected Class<? extends Reducer<Writable, Writable, Writable, Writable> > task = null;
	private transient Reducer<Writable, Writable, Writable, Writable> obj = null;
	
	public UserReduceTaskDescriptor(
		Reducer<Writable, Writable, Writable, Writable> task, TaskType type, int id)
		throws InvalidPlanException {
		super(type, id);
		this.task = (Class<? extends Reducer<Writable, Writable, Writable, Writable> >) task.getClass();
	}
	
	public UserReduceTaskDescriptor(
		Reducer<Writable, Writable, Writable, Writable> task, TaskType type)
		throws InvalidPlanException {
		super(type);
		this.task = (Class<? extends Reducer<Writable, Writable, Writable, Writable> >) task.getClass();
	}
	
	public String toString() {
		return ("UserReduceTask");
	}
	
	public void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		throw new InvalidTaskRequestException("Cannot translate a reducer to a mapper.");
	}

	public void exec(Writable key, Iterator<Writable> values,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.REDUCE)
			throw new InvalidTaskRequestException();
		
		if (obj == null)
			obj = task.newInstance();

		try {
			obj.reduce(key, values, output, reporter);
			
			if (count) {
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_INPUT_RECORDS), 1);
				if (key instanceof Text)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_KEY_BYTES), ((Text) key).getLength());
				if (key instanceof BytesWritable)
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), IndexCounter.TASK_INPUT_KEY_BYTES), ((BytesWritable) key).getLength());
				// No efficient way to calculate TASK_INPUT_VALUE_BYTES

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
					
					/*
					Writable value = pair.second;
					if (((Text) value).toString().startsWith("1000005|"))
						System.out.println(((Text) value).toString() + " in UserReduceTaskDescriptor");
					*/
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
		str += "User Reduce";
		System.out.println(str);
	}

	public Class<? extends Reducer<Writable, Writable, Writable, Writable>> getTask() {
		return task;
	}
}
