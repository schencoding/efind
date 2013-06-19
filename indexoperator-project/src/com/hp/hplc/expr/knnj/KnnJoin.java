package com.hp.hplc.expr.knnj;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.expr.chinacache.ChinaCache;
import com.hp.hplc.expr.chinacache.IPLookup;
import com.hp.hplc.expr.chinacache.ChinaCache.Map;
import com.hp.hplc.expr.chinacache.ChinaCache.Reduce;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.optimizer.Optimizer;

public class KnnJoin extends Configured implements Tool {
	public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
		String row = value.toString();
		String[] fields = row.split("\\t");
		Text k = new Text(fields[0]);
		Text v = new Text(fields[1] + "\t" + fields[2] + "\t" + fields[3]);
		output.collect(k, v);
	}
}


public static void main(String[] args) {		
	int exitCode;
	try {
		exitCode = ToolRunner.run(new KnnJoin(), args);
		System.exit(exitCode);
	} catch (Exception e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
}

@Override
public int run(String[] args) throws Exception {
	if (args.length != 2) {
		System.err.println("Usage: java -jar Test.jar <input path> <output path>");
		System.exit(1);
	}
	
	long start = (new Date()).getTime();

	IndexJobConf conf = new IndexJobConf(getConf());
	conf.setJarByClass(KnnJoin.class);

	FileInputFormat.addInputPath(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	Metastore.META_AVAILABLE_THRES = 10000;			// 100000
	IndexJobConf.REEVALUATE_HTRESHOLD = 21000000;	// 20: true, 2100000000: false
	Optimizer.B_USE_CACHE = true;
	Optimizer.B_USE_RE_PART = false;					// false
	Optimizer.B_USE_NODE_SEL = false;				// false
	
	IndexJobConf.FIXED_PLAN = 1;					// 4 for repart, 5 for node select

	KnnLookup lookup = new KnnLookup();
	lookup.setInputKeyClass(LongWritable.class);
	lookup.setInputValueClass(Text.class);
	lookup.setOutputKeyClass(LongWritable.class);
	lookup.setOutputValueClass(Text.class);
	lookup.addIndex("com.hp.hplc.expr.knnj.KnnIndexAccessor", "Useless");
	conf.addHeadIndexOperator(lookup);

	conf.setMapperClass(Map.class);
	conf.setNumReduceTasks(0);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	
	//conf.setNumReduceTasks(12);

	try {
		//JobClient.runJob(conf);
		conf.waitForCompletion(true);
	} catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
	
	long stop = (new Date()).getTime();
	long duration = stop - start;
	long second = duration / 1000;
	
	System.out.println("Response time: " + second + " sec");
	
	return 0;
}
}