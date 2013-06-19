package com.hp.hplc.mr.driver;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.indexopimpl.KeywordExtensionIndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.indexopimpl.ProfileParallelIndexOperator;
import com.hp.hplc.indexopimpl.SimpleKeywordExtensionIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.TokenizerMapper;
import com.hp.hplc.translator.JobTranslator;

public class MRTestDriver4ParIdxOp extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new MRTestDriver4ParIdxOp(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		IndexJobConf indexJobConf = new IndexJobConf(getConf());
		indexJobConf.setJarByClass(MRTestDriver4ParIdxOp.class);

		ParallelIndexOperator parProfileIdxOp = new ProfileParallelIndexOperator();
		indexJobConf.addHeadIndexOperator(parProfileIdxOp);

		indexJobConf.setMapperClass(TokenizerMapper.class);
		indexJobConf.setMapOutputValueClass(Text.class);

		SimpleIndexOperator keyWordExtIdxOp = new SimpleKeywordExtensionIndexOperator(
				"com.hp.hplc.index.InvertedIndexAccessor",
				"InvertedIndex://localhost/Keyword");
		indexJobConf.addBodyIndexOperator(keyWordExtIdxOp);

		indexJobConf.setReducerClass(FrequencyReducer.class);

		indexJobConf.setInputFormat(KeyValueTextInputFormat.class);

		indexJobConf.setOutputKeyClass(Text.class);
		indexJobConf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(indexJobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(indexJobConf, new Path(args[1]));
		
		indexJobConf.waitForCompletion(true);
		
		return 0;
	}

}
