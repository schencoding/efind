package com.hp.hplc.plan;

import java.util.Vector;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import com.hp.hplc.plan.descriptor.IndexLookupTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPostTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPreTaskDescriptor;
import com.hp.hplc.plan.descriptor.TaskDescriptor;
import com.hp.hplc.util.RetrievableOutputCollector;
import com.hp.hplc.util.Pair;

/**
 * Actual map class used by Hadoop.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-12
 */
public class MapWorker extends MapReduceBase
	implements Mapper<Writable, Writable, Writable, Writable> {	
	private Vector<Vector<TaskDescriptor> > splits = null;
	private int reduceIndex = -1;
	private int splitIndex = -1;
	private Vector<TaskDescriptor> split = null;
	
	public void configure(JobConf conf) {
		try {
			String stringParam = ParamHelper.decode(conf.get(Plan.JOB_PARAMETER_NAME));		
			ByteArrayInputStream ba =
				new ByteArrayInputStream(stringParam.getBytes("ISO-8859-1"));
			ObjectInputStream o = new ObjectInputStream(ba);
			Object[] param = (Object[]) o.readObject();
			
			splits = (Vector<Vector<TaskDescriptor> >) param[0];
			reduceIndex = ((Integer) param[1]).intValue();
			
			splitIndex = Integer.valueOf(conf.get(Plan.SPLIT_PARAMETER_NAME));
			
			assert(0 <= splitIndex && splitIndex < splits.size());
			split = splits.get(splitIndex);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		for (int i = 0; i < splits.size(); i++) {
			Vector<TaskDescriptor> split = splits.get(i);
			for (int j = 0; j < split.size(); j++) {
				TaskDescriptor task = split.get(j);
				if (task instanceof IndexPreTaskDescriptor)
					((IndexPreTaskDescriptor) task).close();
				else if (task instanceof IndexLookupTaskDescriptor)
					((IndexLookupTaskDescriptor) task).close();
				else if (task instanceof IndexPostTaskDescriptor)
					((IndexPostTaskDescriptor) task).close();
			}
		}
	}
	
	public void map(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter)
		throws IOException {
		OutputCollector<Writable, Writable> in = null;
		OutputCollector<Writable, Writable> out = null;
		int i;

		in = new RetrievableOutputCollector<Writable, Writable>();
		in.collect(key, value);
		
		for (i = 0; i < split.size(); i++) {
			TaskDescriptor task = split.get(i);
			
			/*
			if (i == split.size() - 1)
				out = output;
			else
				out = new RetrievableOutputCollector<Writable, Writable>();
			*/
			out = new RetrievableOutputCollector<Writable, Writable>();
			
			Iterator<Pair<Writable, Writable> > itr =
				((RetrievableOutputCollector<Writable, Writable>) in).iterator();
			try {
				while (itr.hasNext()) {
					Pair<Writable, Writable> pair = itr.next();
					task.exec(pair.first, pair.second, out, reporter, true);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			in = out;
		}
		
		Iterator<Pair<Writable, Writable> > itr =
			((RetrievableOutputCollector<Writable, Writable>) out).iterator();
		while (itr.hasNext()) {
			Pair<Writable, Writable> pair = itr.next();
			output.collect(pair.first, pair.second);
			
			/*
			Writable __value = pair.second;
			if (((Text) __value).toString().startsWith("1000005|"))
				System.out.println(((Text) __value).toString() + " in MapWorker");
			*/
		}
	}
}
