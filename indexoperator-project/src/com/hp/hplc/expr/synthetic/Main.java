package com.hp.hplc.expr.synthetic;

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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.optimizer.Optimizer;

/**
 * Synthetic test
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-6-29
 */
public class Main extends Configured implements Tool {
	public static void main(String[] args) {		
		int exitCode;
		try {
			exitCode = ToolRunner.run(new Main(), args);
			System.exit(exitCode);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java -jar Synthetic.jar <input path> <output path> <indexName>");
			System.exit(1);
		}
		
		long start = (new Date()).getTime();

		IndexJobConf conf = new IndexJobConf(getConf());
		conf.setJarByClass(Main.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Metastore.META_AVAILABLE_THRES = 10000;			// 100000
		IndexJobConf.REEVALUATE_HTRESHOLD = 2100000000;	// 20: true, 2100000000: false
		Optimizer.B_USE_CACHE = true;
		Optimizer.B_USE_RE_PART = true;					// false
		Optimizer.B_USE_NODE_SEL = true;				// false
		
		IndexJobConf.FIXED_PLAN = 5;					// 4 for repart, 5 for node select

		IndexOperator indexOperator = new IndexOperator();
		indexOperator.setInputKeyClass(LongWritable.class);
		indexOperator.setInputValueClass(Text.class);
		indexOperator.setOutputKeyClass(LongWritable.class);
		indexOperator.setOutputValueClass(Text.class);
		indexOperator.addIndex("com.hp.hplc.index.CassandraPartitionedIndexAccessor",
			"localhost,9160," + args[2]);
		conf.addHeadIndexOperator(indexOperator);

		conf.setMapperClass(IdentityMapper.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setNumReduceTasks(0);

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

