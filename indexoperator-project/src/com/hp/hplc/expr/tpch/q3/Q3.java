package com.hp.hplc.expr.tpch.q3;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.optimizer.Optimizer;

/**
 * TPCH Q3
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-5-31
 */
public class Q3 extends Configured implements Tool{
	public static class Map extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
			String row = value.toString();
			String[] fields = row.split("\\|");
			assert(fields.length == 4);

			// l_orderkey | o_orderdate | o_shippriority
			String __key = fields[0] + "|" + fields[2] + "|" + fields[3];
			double __value = Double.valueOf(fields[1]);
			//System.out.println("Q3 map: " + __key + " -> " + __value);
			
			output.collect(new Text(__key), new DoubleWritable(__value));
		}
	}

	public static class Reduce extends MapReduceBase
		implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
			double sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				// sum += 1;
				// values.next();
			}
			//System.out.println("Q3 reduce: " + ((Text) key).toString() + " -> " + sum);
			output.collect(key, new DoubleWritable(sum));
		}
	}

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new Q3(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: java -jar Q3.jar <input path> <output path> <p1> <p2>");
			System.exit(1);
		}
		
		long start = (new Date()).getTime();

		IndexJobConf conf = new IndexJobConf(getConf());
		conf.setJarByClass(Q3.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Metastore.META_AVAILABLE_THRES = 10000;			// 100000
		IndexJobConf.REEVALUATE_HTRESHOLD = 21;	// 20: true, 2100000000: false
		Optimizer.B_USE_CACHE = false;
		Optimizer.B_USE_RE_PART = true;					// false
		Optimizer.B_USE_NODE_SEL = true;				// false
		
		IndexJobConf.FIXED_PLAN = 0;					// 4 for repart, 5 for node select

		JoinOrders orders = new JoinOrders();
		orders.setInputKeyClass(LongWritable.class);
		orders.setInputValueClass(Text.class);
		orders.setOutputKeyClass(LongWritable.class);
		orders.setOutputValueClass(Text.class);
		orders.addIndex("com.hp.hplc.index.CassandraPartitionedIndexAccessor", "localhost,9160,tpch_orders10G_" + args[2]);
		conf.addHeadIndexOperator(orders);

		JoinCustomer customer = new JoinCustomer();
		customer.setInputKeyClass(LongWritable.class);
		customer.setInputValueClass(Text.class);
		customer.setOutputKeyClass(LongWritable.class);
		customer.setOutputValueClass(Text.class);
		customer.addIndex("com.hp.hplc.index.CassandraPartitionedIndexAccessor", "localhost,9160,tpch_customer10G_" + args[3]);
		conf.addHeadIndexOperator(customer);
	
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setNumReduceTasks(12);

		try {
			conf.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		long stop = (new Date()).getTime();
		long duration = stop - start;
		long second = duration / 1000;
		
		System.out.println("Response time: " + second + " sec");
		
		return (0);
	}
}

