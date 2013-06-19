package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;
import com.hp.hplc.indexoperator.util.K2V2Writable;

import com.hp.hplc.optimizer.Optimizer;
import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;

import com.hp.hplc.util.LRUCache;
import com.hp.hplc.util.MyLRUCache;
import com.hp.hplc.util.CachingPolicy;
import com.hp.hplc.util.Pair;

/**
 * Descriptor of a index lookup task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-12
 */
public class IndexLookupTaskDescriptor extends TaskDescriptor implements Serializable {
	private static final long serialVersionUID = -5131485592329772401L;
	private static final int CACHE_SIZE = 1024;
	private Class<? extends __IndexOperator> task = null;
	private transient __IndexOperator obj = null;
	private transient Vector<__IndexAccessor> accessors = null;
	private transient Class<? extends Writable>[] valueClasses = null;
	
	private Vector<String> names = null;
	private Vector<String> urls = null;
	
	private int coPartitionIndex = -1;

	private static final CachingPolicy policy = CachingPolicy.KEY_VALUE;
	private transient LRUCache<Pair<Integer, Writable>, Vector<Writable> > cache = null;
	private transient int[] prefix = null;

	public IndexLookupTaskDescriptor(__IndexOperator task, TaskType type, int id)
		throws InvalidPlanException {
		super(type, id);
		this.task = task.getClass();
		this.names = task.getNames();
		this.urls = task.getUrls();
	}
	
	public IndexLookupTaskDescriptor(__IndexOperator task, TaskType type)
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
		return ("IndexLookupTask");
	}
	
	public void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InvalidPlanException,
			InstantiationException, IllegalAccessException {
		System.out.println("Key: " + key.toString());
		System.out.println("Value: " + value.toString());
		if (this.getType() != TaskType.MAP)
			throw new InvalidTaskRequestException();
		
		if (obj == null) {
			obj = task.newInstance();
			for (int i = 0; i < names.size(); i++)
				obj.addIndex(names.get(i), urls.get(i));
			accessors = obj.getInternal();
			valueClasses = new Class [names.size()];
			for (int i = 0; i < names.size(); i++)
				valueClasses[i] = accessors.get(i).getValueClass();
		}
		
		if (cache == null) {
			if (policy == CachingPolicy.KEY_VALUE) {
				cache = new LRUCache<Pair<Integer, Writable>, Vector<Writable> >(CACHE_SIZE);
				prefix = new int [accessors.size()];
				int i, j;
				for (j = 0; j < prefix.length; j++) {
					prefix[j] = j;
					for (i = 0; i < j; i++)
						if (accessors.get(i).getUrl().equals(accessors.get(j).getUrl())) {
							prefix[j] = i;
							break;
						}
				}
			} else if (policy == CachingPolicy.KEY_ONLY) {
				throw new InvalidPlanException("KEY_ONLY is not supported yet.");
			} else
				throw new InvalidPlanException("Undefined caching policy.");
		}

		try {
			Vector<__IndexAccessor> accessors = obj.getInternal();
			IndexInput keys = null;
			IndexOutput values = null;
			Vector<Writable>[] __keys = null;
			K2V2Writable k2v2 = (K2V2Writable) value;
			keys = k2v2.getKeys();

			values = new IndexOutput(keys, valueClasses);
			__keys = keys.getInternal();
			
			long[] index_input_keys = null;
			long[] index_input_bytes = null;
			long[] index_output_values = null;
			long[] index_output_bytes = null;
			long[] index_cache_hit = null;
			long[] index_cache_miss = null;
			long[] index_lookup_time = null;
			
			if (count) {
				index_input_keys = new long [accessors.size()];
				index_input_bytes = new long [accessors.size()];
				index_output_values = new long [accessors.size()];
				index_output_bytes = new long [accessors.size()];
				index_cache_hit = new long [accessors.size()];
				index_cache_miss = new long [accessors.size()];
				index_lookup_time = new long [accessors.size()];
				
				for (int ii = 0; ii < accessors.size(); ii++) {
					index_input_keys[ii] = 0;
					index_input_bytes[ii] = 0;
					index_output_values[ii] = 0;
					index_output_bytes[ii] = 0;
					index_cache_hit[ii] = 0;
					index_cache_miss[ii] = 0;
					index_lookup_time[ii] = 0;
				}
			}

			if (policy == CachingPolicy.KEY_VALUE) {
				for (int i = 0; i < __keys.length; i++) {					// for each index
					for (int j = 0; j < __keys[i].size(); j++) {			// for each key
						Pair<Integer, Writable> index_key =
							new Pair<Integer, Writable>(prefix[i], __keys[i].get(j));
						Vector<Writable> index_value = null;
						boolean bUseCache = false;
						if (true || bUseCache) {
							if (cache.containsKey(index_key)) { // cache hit
								index_value = cache.get(index_key);
								if (count)
									index_cache_hit[i]++;
							} else { // cache miss
								long start = System.currentTimeMillis();
								index_value = accessors.get(i).get(index_key.second);
								long end = System.currentTimeMillis();
								cache.put(index_key, index_value);
								if (count) {
									index_cache_miss[i]++;
									index_lookup_time[i] += (end - start);
								}
							}
						}else{
							index_cache_miss[i]++;
							long start = System.currentTimeMillis();
							index_value = accessors.get(i).get(index_key.second);
							long end = System.currentTimeMillis();
							index_lookup_time[i] += (end - start);
						}
						values.put(i, j, index_value);
						
						if (count) {
							index_input_keys[i]++;
							if (index_key.second instanceof Text)
								index_input_bytes[i] += ((Text) index_key.second).getLength();
							else if (index_key.second instanceof BytesWritable)
								index_input_bytes[i] += ((BytesWritable) index_key.second).getLength();
							if (index_value != null) {
								index_output_values[i] += index_value.size();
								if (index_value.size() > 0) {
									if (index_value.get(0) instanceof Text) {
										for (int ii = 0; ii < index_value.size(); ii++)
											index_output_bytes[i] += ((Text) index_value.get(ii)).getLength();
									} else if (index_value.get(0) instanceof BytesWritable) {
										for (int ii = 0; ii < index_value.size(); ii++)
											index_output_bytes[i] += ((BytesWritable) index_value.get(ii)).getLength();
									}
								}
							}
						}
					} // for j
				} // for i
			} else if (policy == CachingPolicy.KEY_ONLY) {
				throw new InvalidPlanException("KEY_ONLY is not supported yet.");
			} else
				throw new InvalidPlanException("Undefined caching policy.");

			k2v2.setValues(values);

			/*if (coPartitionIndex == -1)
				output.collect(new Text("0"), k2v2);
			else {
				Vector<Writable> key_list = keys.getInternal()[coPartitionIndex];
				if (key_list.size() > 0)
					output.collect(key_list.get(0), k2v2);
				else
					output.collect(new Text("0"), k2v2);
			}*/
			output.collect(key, k2v2);
			
			if (count) {
				for (int ii = 0; ii < accessors.size(); ii++) {
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_INPUT_KEYS), index_input_keys[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_INPUT_BYTES), index_input_bytes[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_OUTPUT_VALUES), index_output_values[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_OUTPUT_BYTES), index_output_bytes[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_CACHE_HIT), index_cache_hit[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
						IndexCounter.get(getID(), ii, IndexCounter.INDEX_CACHE_MISS), index_cache_miss[ii]);
					reporter.incrCounter(IndexCounter.GROUP,
							IndexCounter.get(getID(), ii, IndexCounter.INDEX_LOOKUP_TIME), index_lookup_time[ii]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void exec(Writable key, Iterator<Writable> values,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InvalidPlanException,
			InstantiationException, IllegalAccessException {
		if (this.getType() != TaskType.REDUCE)
			throw new InvalidTaskRequestException();
		
		try {
			/*
			 * We do not count things from output, and it should be safe.
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
		str += "\t" + "IndexLookup " + this.getType() + "\n";
		Iterator<Pair<String, String>> it = this.getProperties().iterator();
		while(it.hasNext()){
			Pair<String, String> pair = it.next();
			if(pair.first.compareToIgnoreCase(JobSplitWriter.INPUT_AND_INDEX_MAPPING)==0)
				continue;
			if(pair.first.compareToIgnoreCase(JobInProgress.REDUCE_AND_INDEX_MAPPING)==0)
				continue;
			str += "\t" + pair.first + " = " + pair.second + "\n";
		}
		System.out.print(str);
	}

	public __IndexOperator getTask() {
		return this.obj;
	}
	
}
