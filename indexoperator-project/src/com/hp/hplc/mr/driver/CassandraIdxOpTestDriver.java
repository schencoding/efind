package com.hp.hplc.mr.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.index.CassandraPartitionedIndexAccessor;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.indexopimpl.OrdersIndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.PseudoMapper;
import com.hp.hplc.mrimpl1.TokenizerMapper;

public class CassandraIdxOpTestDriver extends Configured implements Tool {
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new CassandraIdxOpTestDriver(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		IndexJobConf indexJobConf = new IndexJobConf(getConf());
		indexJobConf.setJarByClass(CassandraIdxOpTestDriver.class);
		
		OrdersIndexOperator ordersIdxOp = new OrdersIndexOperator();
		//String url = "localhost,9160,tpch_orders";
		String url = "localhost,9160,tpch_supplier";
		ordersIdxOp.addIndex(CassandraPartitionedIndexAccessor.class.getName(), url);

		indexJobConf.addHeadIndexOperator(ordersIdxOp);

		indexJobConf.setMapperClass(PseudoMapper.class);
		indexJobConf.setMapOutputKeyClass(Text.class);
		indexJobConf.setMapOutputValueClass(Text.class);
		indexJobConf.setOutputKeyClass(Text.class);
		indexJobConf.setOutputValueClass(Text.class);
		
		//indexJobConf.setReducerClass(FrequencyReducer.class);
		
		//indexJobConf.addTailIndexOperator(profileIdxOp);

		indexJobConf.setInputFormat(TextInputFormat.class);
		indexJobConf.getNumReduceTasks();
		System.out.println(indexJobConf.getOutputFormat().toString());	

		FileInputFormat.addInputPath(indexJobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(indexJobConf, new Path(args[1]));
		
		indexJobConf.waitForCompletion(true);
	
		return 0;
	}

}
