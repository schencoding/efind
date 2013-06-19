package com.hp.hplc.mrimpl1;

import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.index.CassandraPartitionedIndexAccessor;
import com.hp.hplc.index.__IndexAccessor;



public class IndexJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>, Serializable{

	
    /**
	 * 
	 */
	private static final long serialVersionUID = -2559978857285613755L;
	private __IndexAccessor accessor = null;
	
	private String dummyStr = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	
	//private Text word = new Text();
    

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String str = value.toString();
		String[] strs = str.split("\\|");
		String shipDateStr = strs[10];
		if(shipDateStr.compareToIgnoreCase("1995-03-15")<=0){
			return;
		}
		//Text ik = new Text(strs[0]);
		Text ik = new Text(strs[2]);
		Vector<Writable> ivs = accessor.get(ik);
		
		for(int i=0; i<ivs.size(); i++){
			value.set(value.toString() + " | " + ivs.get(i).toString());
			output.collect(ik, value);
		}
	}


	@Override
	public void configure(JobConf job) {
		String url = "localhost,9160,tpch_supplier";
		accessor = new CassandraPartitionedIndexAccessor(url);
		super.configure(job);
	}
}
