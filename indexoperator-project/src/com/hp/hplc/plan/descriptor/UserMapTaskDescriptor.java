package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;
import com.hp.hplc.util.Pair;
import com.hp.hplc.util.RetrievableOutputCollector;

/**
 * Descriptor of a user map task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-11
 */
public class UserMapTaskDescriptor extends TaskDescriptor implements Serializable {
	private static final long serialVersionUID = 4299296331829886688L;
	protected Class<? extends Mapper<Writable, Writable, Writable, Writable> > task = null;
	private transient Mapper<Writable, Writable, Writable, Writable> obj = null;
	
	public UserMapTaskDescriptor(
		Mapper<Writable, Writable, Writable, Writable> task, TaskType type, int id)
		throws InvalidPlanException {
		super(type, id);
		this.task =
			(Class<? extends Mapper<Writable, Writable, Writable, Writable> >) task.getClass();
	}
	
	public UserMapTaskDescriptor(
		Mapper<Writable, Writable, Writable, Writable> task, TaskType type)
		throws InvalidPlanException {
		super(type);
		this.task =
			(Class<? extends Mapper<Writable, Writable, Writable, Writable> >) task.getClass();
	}
	
	public String toString() {
		return ("UserMapTask");
	}
	
	public void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.MAP)
			throw new InvalidTaskRequestException();
		
		if (obj == null)
			obj = task.newInstance();

		try {			
			obj.map(key, value, output, reporter);
			
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
					Writable __value = pair.second;
					if (((Text) __value).toString().startsWith("1000005|"))
						System.out.println(((Text) __value).toString() + " in UserMapTaskDescriptor");
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

	public void exec(Writable key, Iterator<Writable> values,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.REDUCE)
			throw new InvalidTaskRequestException();

		try {
			long task_input_records = 0;
			long task_input_key_bytes = 0;
			long task_input_value_bytes = 0;
			
			while (values.hasNext()) {
				Writable value = values.next();
				exec(key, value, output, reporter, false);
				
				if (count) {
					task_input_records++;
					if (key instanceof Text)
						task_input_key_bytes += ((Text) key).getLength();
					else if (key instanceof BytesWritable)
						task_input_key_bytes += ((BytesWritable) key).getLength();
					if (value instanceof Text)
						task_input_value_bytes += ((Text) value).getLength();
					else if (value instanceof BytesWritable)
						task_input_value_bytes += ((BytesWritable) value).getLength();
				}
			}

			if (count) {
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_INPUT_RECORDS), task_input_records);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_INPUT_KEY_BYTES), task_input_key_bytes);
				reporter.incrCounter(IndexCounter.GROUP,
					IndexCounter.get(getID(), IndexCounter.TASK_INPUT_VALUE_BYTES), task_input_value_bytes);

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
		str += "User map";
		System.out.println(str);
	}

	public Class<? extends Mapper<Writable, Writable, Writable, Writable>> getTask() {
		return this.task;
	}
}

