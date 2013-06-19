package com.hp.hplc.mr.driver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexopimpl.KeywordExtensionIndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.indexopimpl.SimpleKeywordExtensionIndexOperator;
import com.hp.hplc.indexopimpl.SimpleProfileIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.TokenizerMapper;
import com.hp.hplc.translator.JobTranslator;

public class MRTestDriver extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		LinkedList<String> list = new LinkedList<String>();
		list.add("1");
		list.add("2");
		list.add("3");
		list.add("4");
		list.add("5");
		list.add("6");
		Iterator<String> it1 = list.iterator();
		it1.next();
		it1.next();
		it1.next();
		String str1 = it1.next();
		
		System.out.println(str1);
		
		
		Iterator<String> it2 = list.iterator();
		String str2 = it2.next();
		System.out.println(str2);
		
		
		
		//int exitCode = ToolRunner.run(new MRTestDriver(), args);
		//System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		IndexJobConf indexJobConf = new IndexJobConf(getConf());
		indexJobConf.setJarByClass(MRTestDriver.class);

		SimpleIndexOperator profileIdxOp = new SimpleProfileIndexOperator(
				"com.hp.hplc.index.HashIndexAccessor",
				"Hash://localhost/Profile");
		indexJobConf.addHeadIndexOperator(profileIdxOp);

		indexJobConf.setMapperClass(TokenizerMapper.class);
		indexJobConf.setMapOutputKeyClass(Text.class);
		//indexJobConf.setMapOutputValueClass(theClass)
		indexJobConf.setMapOutputValueClass(Text.class);
		indexJobConf.setNumReduceTasks(0);
		
		//indexJobConf.setPartitionerClass(theClass);

		//SimpleIndexOperator keyWordExtIdxOp = new SimpleKeywordExtensionIndexOperator(
		//		"com.hp.hplc.index.InvertedIndexAccessor",
		//		"InvertedIndex://localhost/Keyword");
		//indexJobConf.addBodyIndexOperator(keyWordExtIdxOp);

		//indexJobConf.setReducerClass(FrequencyReducer.class);

		indexJobConf.setInputFormat(KeyValueTextInputFormat.class);
		indexJobConf.getNumReduceTasks();
		indexJobConf.getReducerClass();
		
		//indexJobConf.getMapOutputKeyClass();

		//indexJobConf.setOutputKeyClass(Writable.class);
		//indexJobConf.setOutputValueClass(IntWritable.class);
	

		FileInputFormat.addInputPath(indexJobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(indexJobConf, new Path(args[1]));
		
		indexJobConf.waitForCompletion(true);
	
		return 0;
	}

}
