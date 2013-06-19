package com.hp.hplc.mrimpl1;

import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class PseudoMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>, Serializable{

	
    /**
	 * 
	 */
	private static final long serialVersionUID = -2559978857285613755L;
	
	
	//private Text word = new Text();
    

	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/*Text word = new Text();
		String[] strs = value.toString().split("\\|");
		for(int i=0; i< strs.length; i++){
			word.set(strs[i]);
	        output.collect(key, word);
		}*/
		
		output.collect(key, value);
	}
}
