package com.hp.hplc.mr.driver;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.mapred.TaskCompletionEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
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

public class SimpleMRTestDriver extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		//int exitCode = ToolRunner.run(new SimpleMRTestDriver(), args);
		//System.exit(exitCode);
		
		IntWritable i1 = new IntWritable(10);
		IntWritable i2 = new IntWritable(10);
		
		Text t1 = new Text("0");
		Text t2 = new Text("t1");
		LongWritable lw = new LongWritable();
		System.out.println();
		if(t1.equals( t2))
			System.out.println("Equal");
		

	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass(SimpleMRTestDriver.class);

		jobConf.setMapperClass(TokenizerMapper.class);
		jobConf.setMapOutputKeyClass(Text.class);
		//indexJobConf.setMapOutputValueClass(theClass)
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setNumReduceTasks(2);
		
		//indexJobConf.setPartitionerClass(theClass);

		//SimpleIndexOperator keyWordExtIdxOp = new SimpleKeywordExtensionIndexOperator(
		//		"com.hp.hplc.index.InvertedIndexAccessor",
		//		"InvertedIndex://localhost/Keyword");
		//indexJobConf.addBodyIndexOperator(keyWordExtIdxOp);

		jobConf.setReducerClass(FrequencyReducer.class);

		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.getNumReduceTasks();
		jobConf.getReducerClass();
		
		//indexJobConf.getMapOutputKeyClass();

		//indexJobConf.setOutputKeyClass(Writable.class);
		//indexJobConf.setOutputValueClass(IntWritable.class);
	

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		//jobConf.set("jobclient.output.filter", "ALL");
		JobClient jc = new JobClient(jobConf);
		RunningJob job = jc.submitJob(jobConf);
		
		//Job job = new Job(jobConf);
		//job.submit();
		int eventCounter = 0;
		while(!job.isComplete()){
			Thread.sleep(5*1000);
			TaskCompletionEvent[] events = 
			        job.getTaskCompletionEvents(eventCounter); 
			      eventCounter += events.length;
			      for(TaskCompletionEvent event : events){
			    	  System.out.println(event.toString());
			      }
			      
			      //job.killJob();
		}
		//job.waitForCompletion(true);
	
		return 0;
	}

}
