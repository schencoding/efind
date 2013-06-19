package com.hp.hplc.mr.driver;

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
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.translator.Mapper4Mapper;

public class ChainMRTestDriver extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ChainMRTestDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(getConf());
	    jobConf.setJarByClass(ChainMRTestDriver.class);
	    
	    jobConf.setOutputKeyClass(Text.class);
	    jobConf.setOutputValueClass(IntWritable.class);
	    
	    JobConf mapAConf = new JobConf(false);
	    mapAConf.set("mapper.mapperclassName", "com.hp.hplc.MyMapperOld");
	    Class mapAOutputKeyClass = jobConf.getMapOutputKeyClass();
	    System.out.println("Map Output Key Class: " + mapAOutputKeyClass);
	    
	    Class mapAOutputValueClass = jobConf.getMapOutputValueClass();
	    System.out.println("Map Output Value Class: " + mapAOutputValueClass);
	    
	    ChainMapper.addMapper(jobConf, Mapper4Mapper.class, Writable.class, Writable.class, mapAOutputKeyClass, mapAOutputValueClass, true, mapAConf);
	    
//	    ChainReducer.setReducer(job, klass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, byValue, reducerConf);
//	    ChainReducer.addMapper(job, klass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, byValue, mapperConf)
	    	    
	    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
	   
	    Job job = new Job(jobConf);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    return 0;
	}

}
