package com.hp.hplc.mr.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.TokenizerMapper;
import com.hp.hplc.optimizer.Optimizer;

public class IdxOpTestDriver extends Configured implements Tool {
	
	/**
	 * @param args
	 * args[0] input path
	 * args[1] output path
	 * args[2] META_AVAILABLE_THRES
	 * args[3] re-evaluate plan after # of maps 
	 * args[4] B_Cache
	 * args[5] B_Part
	 * args[6] B_Node
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new IdxOpTestDriver(), args);
		
		
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {		
		//The following code should be added to every test driver
		//------------begin---------------
		IndexJobConf indexJobConf = new IndexJobConf(getConf());
		indexJobConf.setJarByClass(IdxOpTestDriver.class);
		Metastore.META_AVAILABLE_THRES = Integer.parseInt(args[2]);
		IndexJobConf.REEVALUATE_HTRESHOLD = Integer.parseInt(args[3]);
		Optimizer.B_USE_CACHE = Boolean.parseBoolean(args[4]);
		Optimizer.B_USE_RE_PART = Boolean.parseBoolean(args[5]);
		Optimizer.B_USE_NODE_SEL = Boolean.parseBoolean(args[6]);
		//------------end-----------------
		
		
		ProfileIndexOperator profileIdxOp = new ProfileIndexOperator();
		profileIdxOp.addIndex("com.hp.hplc.index.__HashIndexAccessor",
				"Hash://localhost/Profile");
		profileIdxOp.addIndex("com.hp.hplc.index.__HashIndexAccessor",
				"Hash://localhost/Profile1");

		profileIdxOp.addCoPartitionedWithIndex(0);

		indexJobConf.addHeadIndexOperator(profileIdxOp);

		indexJobConf.setMapperClass(TokenizerMapper.class);
		indexJobConf.setMapOutputKeyClass(Text.class);
		indexJobConf.setMapOutputValueClass(Text.class);
		indexJobConf.setOutputKeyClass(Text.class);
		indexJobConf.setOutputValueClass(Text.class);
		
		indexJobConf.setReducerClass(FrequencyReducer.class);
		
		//indexJobConf.addTailIndexOperator(profileIdxOp);

		indexJobConf.setInputFormat(KeyValueTextInputFormat.class);
		indexJobConf.getNumReduceTasks();
		System.out.println(indexJobConf.getOutputFormat().toString());	

		FileInputFormat.addInputPath(indexJobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(indexJobConf, new Path(args[1]));
		
		indexJobConf.waitForCompletion(true);
	
		return 0;
	}

}
