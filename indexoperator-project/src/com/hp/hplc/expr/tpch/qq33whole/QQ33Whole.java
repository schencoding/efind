package com.hp.hplc.expr.tpch.qq33whole;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import com.hp.hplc.util.Pair;
import com.hp.hplc.util.RetrievableOutputCollector;

/**
 * TPCH Q3
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-5-31
 */
public class QQ33Whole extends Configured implements Tool {
	public static class Map extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			String row = value.toString();
			String[] fields = row.split("\\|");

			// l_orderkey | o_orderdate | o_shippriority
			String __key = fields[0] + "|" + fields[20] + "|" + fields[23];
			
			output.collect(new Text(__key), value);
			
			if (((Text) value).toString().startsWith("1000005|"))
				System.out.println(((Text) value).toString() + " in Map");
		}
	}

	public static class Reduce extends MapReduceBase
		implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			while (values.hasNext()) {
				Text value = values.next();
				// output.collect(key, value);
				output.collect(key, new Text(value.toString()));
				
				if (((Text) value).toString().startsWith("1000005|"))
					System.out.println(((Text) value).toString() + " in Reduce");
			}
			
			RetrievableOutputCollector<Text, Text> out = (RetrievableOutputCollector<Text, Text>) output;
			Iterator<Pair<Text, Text> > itr = out.iterator();
			while (itr.hasNext()) {
				Pair<Text, Text> pair = itr.next();
				Writable value = pair.second;
				if (((Text) value).toString().startsWith("1000005|"))
					System.out.println(((Text) value).toString() + " in Reduce (scan)");
			}
		}
	}

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new QQ33Whole(), args);
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

		IndexJobConf conf = new IndexJobConf(getConf());
		conf.setJarByClass(QQ33Whole.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Metastore.META_AVAILABLE_THRES = 10000;			// 100000
		IndexJobConf.REEVALUATE_HTRESHOLD = 200000;		// 20: true, 2100000000: false
		Optimizer.B_USE_RE_PART = true;					// false
		Optimizer.B_USE_NODE_SEL = true;				// false

		JoinOrders orders = new JoinOrders();
		orders.setInputKeyClass(LongWritable.class);
		orders.setInputValueClass(Text.class);
		orders.setOutputKeyClass(LongWritable.class);
		orders.setOutputValueClass(Text.class);
		orders.addIndex("com.hp.hplc.index.CassandraPartitionedIndexAccessor", "localhost,9160,tpch_orders1G_" + args[2]);
		conf.addHeadIndexOperator(orders);

		JoinCustomer customer = new JoinCustomer();
		customer.setInputKeyClass(LongWritable.class);
		customer.setInputValueClass(Text.class);
		customer.setOutputKeyClass(LongWritable.class);
		customer.setOutputValueClass(Text.class);
		customer.addIndex("com.hp.hplc.index.CassandraPartitionedIndexAccessor", "localhost,9160,tpch_customer1G_" + args[3]);
		conf.addHeadIndexOperator(customer);
	
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		try {
			conf.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return (0);
	}
}

