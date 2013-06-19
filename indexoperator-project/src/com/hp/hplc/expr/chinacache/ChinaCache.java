package com.hp.hplc.expr.chinacache;

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

import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.optimizer.Optimizer;

/**
 * China cache, IP lookup
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-6-19
 */
public class ChinaCache extends Configured implements Tool {
	public static class Map extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		
		public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			output.collect(value, ONE);
		}
	}

	public static class Reduce extends MapReduceBase
		implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			int sum = 0;
			while (values.hasNext())
				sum += values.next().get();
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) {		
		int exitCode;
		try {
			exitCode = ToolRunner.run(new ChinaCache(), args);
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
		conf.setJarByClass(ChinaCache.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Metastore.META_AVAILABLE_THRES = 10000;			// 100000
		IndexJobConf.REEVALUATE_HTRESHOLD = 21;	// 20: true, 2100000000: false
		Optimizer.B_USE_CACHE = false;
		Optimizer.B_USE_RE_PART = true;					// false
		Optimizer.B_USE_NODE_SEL = false;				// false
		
		IndexJobConf.FIXED_PLAN = 0;					// 4 for repart, 5 for node select

		IPLookup ip = new IPLookup();
		ip.setInputKeyClass(LongWritable.class);
		ip.setInputValueClass(Text.class);
		ip.setOutputKeyClass(LongWritable.class);
		ip.setOutputValueClass(Text.class);
		ip.addIndex("com.hp.hplc.index.IPLookupIndexAccessor", "Useless");
		conf.addHeadIndexOperator(ip);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setNumReduceTasks(12);

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

