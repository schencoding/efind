package com.hp.hplc.mr.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import com.hp.hplc.index.CassandraPartitionedIndexAccessor;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.indexopimpl.OrdersIndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.IndexJoinMapper;
import com.hp.hplc.mrimpl1.PseudoMapper;
import com.hp.hplc.mrimpl1.TokenizerMapper;

public class IndexJoinTestDriver extends Configured implements Tool {
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new IndexJoinTestDriver(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf());
		conf.setJarByClass(IndexJoinTestDriver.class);		

		conf.setMapperClass(IndexJoinMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		

		conf.setInputFormat(TextInputFormat.class);
	
		System.out.println(conf.getOutputFormat().toString());	

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Job job = new Job(conf);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		return 0;
	}

}
