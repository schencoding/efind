package com.hp.hplc.optimizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;

//import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.K2V2Writable;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.IndexMetadata;
import com.hp.hplc.metadata.IndexOperatorMetadata;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.metadata.UserMRMetadata;
import com.hp.hplc.mrimpl1.SystemReducer;

import com.hp.hplc.plan.Plan;
import com.hp.hplc.plan.descriptor.IndexLookupTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPostTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPreTaskDescriptor;
import com.hp.hplc.plan.descriptor.SystemReduceTaskDescriptor;
import com.hp.hplc.plan.descriptor.TaskDescriptor;
import com.hp.hplc.plan.descriptor.TaskType;
import com.hp.hplc.plan.descriptor.UserMapTaskDescriptor;
import com.hp.hplc.plan.descriptor.UserReduceTaskDescriptor;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.JobNotFoundException;
import com.hp.hplc.plan.exception.TaskNotFoundException;

public class Optimizer {

	public static boolean B_USE_CACHE = false;
	public static boolean B_USE_RE_PART = true;
	public static boolean B_USE_NODE_SEL = true;
	public static int INDEX_LOOKUP_COMMON_COST = 50;
	public static int LOC_BYTES_PER_SECOND = 1200;
	public static int THRESH_CHANGE_PLAN = 1000;
	public static Metastore metastore = new Metastore();
	public static int FIXED_SHUFF_COST = 1000000;
	public static int numOfRecords = 6000;

	private IndexJobConf indexJobConf;

	public Optimizer(IndexJobConf indexJobConf) {
		this.indexJobConf = indexJobConf;
	}
	
	public List<String> executeCommand(String cmd){
		List<String> ret = new ArrayList<String>();
		
		Process p;
		try {
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor(); 
			
			BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				ret.add(line);
				line=reader.readLine(); 
			} 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		System.out.println("Done"); 
		
	return ret;
	}
	
	public long getInputSize() {
		long size = 0;

		Path path = FileInputFormat.getInputPaths(this.indexJobConf)[0];
		System.out.println("path.toString() = " + path.toString());
		List<String> result = executeCommand("hadoop fs -ls " + path.toString());
		for (int i = 1; i < result.size(); i++) {
			String str = result.get(i);
			String[] fields = str.split(" ");
			for(int j=0; j<fields.length; j++){
				System.out.println(fields[j]);
			}
			System.out.println("size: " + fields[fields.length - 4]);
			size += Long.parseLong(fields[fields.length - 4]);
		}

		System.out.println("Lenth of input: " + size);

		return size;
	}

	
	public Plan optimize(int fixedPlan) throws Exception {
		Plan plan = new Plan();
		//Add the first job, because there should be at least one job
		plan.addJob();
		plan.setInputFormat(this.indexJobConf.getInputFormat().getClass());

		//Just set a pseudo number to avoid small number
		//int numOfRecords = 6000*10;
		numOfRecords = (int)(getInputSize()/1200000);
		
		int currNumOfRecords = numOfRecords;
		System.out.println("Input lines: " + currNumOfRecords);
		
		Class mapInputKeyCls = null;
		Class mapInputValueCls = null;

		//IndexOperators before User Map
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators().iterator();
		int idxOpCounter = 0;
		while (itHead.hasNext()) {
			Object obj = itHead.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;

				int[] planCode = new int[2];
				this.selectPlan(idxOp, planCode, currNumOfRecords);
				if(idxOpCounter == 0 && fixedPlan != 0){
					planCode[0] = fixedPlan;
					if(fixedPlan == 4){
						planCode[1] = 0; 
					}
					if(fixedPlan == 5){
						planCode[1] = 0; 
					}
				}
				idxOpCounter ++;
				System.out.println("Plan for " + idxOp + " is " + planCode[0]);
				this.addToPlan(plan, idxOp, planCode);
				mapInputKeyCls = idxOp.getOutputKeyClass();
				mapInputValueCls = idxOp.getOutputValueClass();
				
				IndexOperatorMetadata idxOpMeta = this.metastore.getIdxOpMeta(idxOp.getClass().toString());
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getSeleOfPrePro());
				currNumOfRecords = (int)(((double)currNumOfRecords) *idxOpMeta.getPostProductivity());
				
				System.out.println("Current lines: " + currNumOfRecords);
				

			} else if (obj instanceof ParallelIndexOperator) {
				ParallelIndexOperator pIdxOp = (ParallelIndexOperator) obj;
				ParallelPlan pPlan = this.selectPlan(pIdxOp);
				this.addToPlan(plan, pIdxOp, pPlan);
			} else {
				System.err.println("Unkown index operator!");
			}
		}

		// user Map
		if (indexJobConf.getMapperClass() != null) {
			try {
				Mapper mapper = indexJobConf.getMapperClass().getConstructor().newInstance();

				this.addMapToPlan(plan, mapper, mapInputKeyCls, mapInputValueCls);
				
				UserMRMetadata mrMeta = this.metastore.getMRMeta(indexJobConf.getMapperClass().getName());
				currNumOfRecords = (int)(((double)currNumOfRecords) * mrMeta.getProductivity());
				
				System.out.println("Current lines: " + currNumOfRecords);
				
				
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
		} else {
			return plan;
		}

		// Index Operators between user map and user reduce
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators().iterator();
		while (itBody.hasNext()) {
			Object obj = itBody.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;
				int[] planCode = new int[2];
				this.selectPlan(idxOp, planCode, currNumOfRecords);
				if(fixedPlan != 0){
					planCode[0] = fixedPlan;
					if(fixedPlan == 4){
						planCode[1] = 0; 
					}
					if(fixedPlan == 5){
						planCode[1] = 0; 
					}
				}
				System.out.println("Plan for " + idxOp + " is " + planCode[0]);
				this.addToPlan(plan, idxOp, planCode);
				
				IndexOperatorMetadata idxOpMeta = this.metastore.getIdxOpMeta(idxOp.getClass().toString());
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getSeleOfPrePro());
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getPostProductivity());
				
				System.out.println("Current lines: " + currNumOfRecords);
			} else if (obj instanceof ParallelIndexOperator) {
				ParallelIndexOperator pIdxOp = (ParallelIndexOperator) obj;
				ParallelPlan pPlan = this.selectPlan(pIdxOp);
				this.addToPlan(plan, pIdxOp, pPlan);
			} else {
				System.err.println("Unkown index operator!");
			}
		}

		// User Reduce
		if (indexJobConf.getReducerClass() != null) {
			try {
				Reducer reducer = (Reducer) indexJobConf.getReducerClass().getConstructor().newInstance();
				this.addReduceToPlan(plan, reducer);
				
				UserMRMetadata mrMeta = this.metastore.getMRMeta(indexJobConf.getReducerClass().getName());
				currNumOfRecords = (int)(((double)currNumOfRecords) * mrMeta.getProductivity());
				
				System.out.println("Current lines: " + currNumOfRecords);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
		} else {
			return plan;
		}

		// Index operators after user reduce
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators().iterator();
		while (itTail.hasNext()) {
			Object obj = itTail.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;
				int[] planCode = new int[2];
				this.selectPlan(idxOp, planCode, currNumOfRecords);
				if(fixedPlan != 0){
					planCode[0] = fixedPlan;
					if(fixedPlan == 4){
						planCode[1] = 0; 
					}
					if(fixedPlan == 5){
						planCode[1] = 0; 
					}
				}
				System.out.println("Plan for " + idxOp + " is " + planCode[0]);
				this.addToPlan(plan, idxOp, planCode);
				
				IndexOperatorMetadata idxOpMeta = this.metastore.getIdxOpMeta(idxOp.getClass().toString());
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getSeleOfPrePro());
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getPostProductivity());
				
				System.out.println("Current lines: " + currNumOfRecords);
			} else if (obj instanceof ParallelIndexOperator) {
				ParallelIndexOperator pIdxOp = (ParallelIndexOperator) obj;
				ParallelPlan pPlan = this.selectPlan(pIdxOp);
				this.addToPlan(plan, pIdxOp, pPlan);
			} else {
				System.err.println("Unkown index operator!");
			}
		}

		return plan;
	}
	
	public long generatePlan(Plan plan, __IndexOperator idxOp, int numOfInputRecord) throws Exception{
		int[] planCode = new int[2];
		long cost = this.selectPlan(idxOp, planCode, numOfInputRecord);
		
		//System.out.println("InputKeyClass: " + idxOp.getInputKeyClass().getName());
		//System.out.println("InputValueClass: " + idxOp.getInputValueClass().getName());
		//System.out.println("OutputKeyClass: " + idxOp.getOutputKeyClass().getName());
		//System.out.println("OutputValueClass: " + idxOp.getOutputValueClass().getName());
		
		this.addToPlan(plan, idxOp, planCode);
		return cost;
	}

	private long getInitialPlan(__IndexOperator idxOp, IndexOperatorMetadata idxOpMeta, int[] planCode, int numOfInputRecord) {
		
		
		/*int sizeOfInputRecord = idxOpMeta.getSizeOfInputRecord();
		
		int numOfIndexes = idxOpMeta.getNumOfIndexes();
		int maxIdxIOSize = Integer.MAX_VALUE;
		int selectedNodeWithMaxIdxIOSize = -1;
		for(int i=0; i< numOfIndexes; i++){
			String accessStr = idxOp.getAccessStr(i);
			IndexMetadata idxMeta = this.metastore.getIdxMeta(accessStr);
			int sizeOfIndexKey = idxMeta.getAvgKeySize();
			int sizeOfIndexValue = idxMeta.getAvgValueSize();
			if(maxIdxIOSize > (sizeOfIndexKey + sizeOfIndexValue)){
				maxIdxIOSize = (sizeOfIndexKey + sizeOfIndexValue);
			}
			selectedNodeWithMaxIdxIOSize = i;
		}
		if(sizeOfInputRecord > maxIdxIOSize){
			result[0] = 1;
			result[1] = -1;
		}else{
			result[0] = 2;
			result[1] = selectedNodeWithMaxIdxIOSize;
		}*/
		planCode[0] = 1;
		planCode[1] = -1;
		return this.getInitPlanCost(idxOp, numOfInputRecord);
	}



	private void addToPlan(Plan plan, __IndexOperator idxOp, int[] idxOpPlan) throws Exception {
		int planId = idxOpPlan[0];
		int planNodeIndicator = idxOpPlan[1];

		if (planId == 1) {
			IndexPreTaskDescriptor preTask = new IndexPreTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(preTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass()); 
			plan.setInputValueClass(idxOp.getInputValueClass());
			plan.setOutputKeyClass(idxOp.getInputKeyClass());
			plan.setOutputValueClass(K2V2Writable.class);

			IndexLookupTaskDescriptor lookupTask = new IndexLookupTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(lookupTask);
			
			setLookupTaskIOType(plan, idxOp.getInputKeyClass(), idxOp.getInputKeyClass());		

			IndexPostTaskDescriptor postTask = new IndexPostTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(postTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass());
			plan.setInputValueClass(K2V2Writable.class);
			plan.setOutputKeyClass(idxOp.getOutputKeyClass());
			plan.setOutputValueClass(idxOp.getOutputValueClass());
			

		} else if (planId == 2) {
			IndexPreTaskDescriptor preTask = new IndexPreTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(preTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass()); 
			plan.setInputValueClass(idxOp.getInputValueClass());
			plan.setOutputKeyClass(idxOp.getInputKeyClass());
			plan.setOutputValueClass(K2V2Writable.class);

			IndexLookupTaskDescriptor lookupTask = new IndexLookupTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(lookupTask);
			
			setLookupTaskIOType(plan, idxOp.getInputKeyClass(), idxOp.getInputKeyClass());		
			//setLookupTaskIOType(plan);	

			IndexPostTaskDescriptor postTask = new IndexPostTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(postTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass());
			plan.setInputValueClass(K2V2Writable.class);
			plan.setOutputKeyClass(idxOp.getOutputKeyClass());
			plan.setOutputValueClass(idxOp.getOutputValueClass());
			
			//set where to execute map
			lookupTask.set(JobSplitWriter.B_RUN_MAP_AT_INDEX, "TRUE");
			lookupTask.set(JobSplitWriter.RUN_MAP_AT_INDEX_ID, Integer.toString(planNodeIndicator));
			__IndexAccessor accessor = idxOp.getInternal().get(planNodeIndicator);
			lookupTask.set(JobSplitWriter.INPUT_AND_INDEX_MAPPING, this.metastore.getInputAndIndexMappingStr(this.indexJobConf, accessor));
			

		} else if (planId == 3) {
			IndexPreTaskDescriptor preTask = new IndexPreTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(preTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass()); 
			plan.setInputValueClass(idxOp.getInputValueClass());
			plan.setOutputKeyClass(idxOp.getInputKeyClass());
			plan.setOutputValueClass(K2V2Writable.class);
			
			plan.setNumOfReduces(0);
//			preTask.set("mapred.reduce.tasks", 0);

			plan.addJob();
			// TODO: set number of reducer to 0 to avoid shuffle
			IndexLookupTaskDescriptor lookupTask = new IndexLookupTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(lookupTask);
			
			setLookupTaskIOType(plan, idxOp.getInputKeyClass(), idxOp.getInputKeyClass());		
			//setLookupTaskIOType(plan);	

			IndexPostTaskDescriptor postTask = new IndexPostTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(postTask);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass());
			plan.setInputValueClass(K2V2Writable.class);
			plan.setOutputKeyClass(idxOp.getOutputKeyClass());
			plan.setOutputValueClass(idxOp.getOutputValueClass());

			//set where to execute map
			lookupTask.set(JobSplitWriter.B_RUN_MAP_AT_INDEX, "TRUE");
			lookupTask.set(JobSplitWriter.RUN_MAP_AT_INDEX_ID, Integer.toString(planNodeIndicator));
			__IndexAccessor accessor = idxOp.getInternal().get(planNodeIndicator);
			lookupTask.set(JobSplitWriter.INPUT_AND_INDEX_MAPPING, this.metastore.getInputAndIndexMappingStr(this.indexJobConf, accessor));

		} else if (planId == 4) {

			IndexPreTaskDescriptor preTask = new IndexPreTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(preTask);
			preTask.setCoPartitionIndex(planNodeIndicator);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass()); 
			plan.setInputValueClass(idxOp.getInputValueClass());
			plan.setOutputKeyClass(Text.class);
			plan.setOutputValueClass(K2V2Writable.class);
			
			int numReduceTasks = idxOp.getInternal().get(planNodeIndicator).getNumberOfPartitions();
			//int numReduceTasks = indexJobConf.getNumReduceTasks();
			plan.setNumOfReduces(32);
			
			SystemReducer ir = new SystemReducer();
			SystemReduceTaskDescriptor srtd = new SystemReduceTaskDescriptor(ir, TaskType.REDUCE);
			plan.addTask(srtd);
			srtd.setInputKeyClass(Text.class);
			srtd.setInputValueClass(K2V2Writable.class);
			srtd.setOutputKeyClass(Text.class);
			srtd.setOutputValueClass(K2V2Writable.class);

			plan.addJob();
			IndexLookupTaskDescriptor lookupTask = new IndexLookupTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(lookupTask);
			
			setLookupTaskIOType(plan, Text.class, Text.class);		
			//setLookupTaskIOType(plan);						

			IndexPostTaskDescriptor postTask = new IndexPostTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(postTask);
			
			plan.setInputKeyClass(Text.class);
			plan.setInputValueClass(K2V2Writable.class);
			plan.setOutputKeyClass(idxOp.getOutputKeyClass());
			plan.setOutputValueClass(idxOp.getOutputValueClass());

			//Run reduce at the original Hadoop selected node, so don't need to set node information			

		} else if (planId == 5) {
			IndexPreTaskDescriptor preTask = new IndexPreTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(preTask);
			preTask.setCoPartitionIndex(planNodeIndicator);
			
			plan.setInputKeyClass(idxOp.getInputKeyClass()); 
			plan.setInputValueClass(idxOp.getInputValueClass());
			plan.setOutputKeyClass(Text.class);
			plan.setOutputValueClass(K2V2Writable.class);

			__IndexAccessor accessor = idxOp.getInternal().get(planNodeIndicator);
			if(!accessor.isPartitioned()){
				throw new Exception("Wrong plan");
			}
			plan.setNumOfReduces(accessor.getNumberOfPartitions());
			//UserReduceTaskDescriptor 
			//plan.addTask(task);
			
			SystemReducer ir = new SystemReducer();
			SystemReduceTaskDescriptor srtd = new SystemReduceTaskDescriptor(ir, TaskType.REDUCE);
			plan.addTask(srtd);
			srtd.setInputKeyClass(Text.class);
			srtd.setInputValueClass(K2V2Writable.class);
			srtd.setOutputKeyClass(Text.class);
			srtd.setOutputValueClass(K2V2Writable.class);
			
			plan.addJob(); //moved to the end just for test.
			IndexLookupTaskDescriptor lookupTask = new IndexLookupTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(lookupTask);
			
			setLookupTaskIOType(plan, Text.class, Text.class);		
			//setLookupTaskIOType(plan);	

			IndexPostTaskDescriptor postTask = new IndexPostTaskDescriptor(idxOp, TaskType.MAP);
			plan.addTask(postTask);
			
			plan.setInputKeyClass(Text.class);
			plan.setInputValueClass(K2V2Writable.class);
			plan.setOutputKeyClass(idxOp.getOutputKeyClass());
			plan.setOutputValueClass(idxOp.getOutputValueClass());
			
			//set where to run reduce
			lookupTask.set(JobSplitWriter.B_RUN_MAP_AT_INDEX, "TRUE");
			lookupTask.set(JobSplitWriter.RUN_MAP_AT_INDEX_ID, Integer.toString(planNodeIndicator));	
			//__IndexAccessor accessor = idxOp.getInternal().get(planNodeIndicator);
			lookupTask.set(JobSplitWriter.INPUT_AND_INDEX_MAPPING, this.metastore.getPartitionAndIndexMappingStr(accessor));


			//plan.addJob();
			/*//set where to run reduce
			lookupTask.set(JobInProgress.B_RUN_REDUCE_AT_INDEX, "TRUE");
			lookupTask.set(JobInProgress.RUN_REDUCE_AT_INDEX, Integer.toString(planNodeIndicator));	
			//__IndexAccessor accessor = idxOp.getInternal().get(planNodeIndicator);
			lookupTask.set(JobInProgress.REDUCE_AND_INDEX_MAPPING, this.metastore.getPartitionAndIndexMappingStr(accessor));
*/
		} else {
			System.out.println("Error plan!!!");
		}

		
	}

	private void setLookupTaskIOType(Plan plan, Class inputkeyCls, Class outputKeyCls) throws JobNotFoundException, TaskNotFoundException {
		plan.setInputKeyClass(inputkeyCls);
		plan.setInputValueClass(K2V2Writable.class);
		plan.setOutputKeyClass(outputKeyCls);
		plan.setOutputValueClass(K2V2Writable.class);
		
	}


	private void addMapToPlan(Plan plan, Mapper mapper, Class mapInputKeyCls, Class mapInputValueCls) throws JobNotFoundException, InvalidPlanException, TaskNotFoundException {
		plan.addTask(new UserMapTaskDescriptor(mapper, TaskType.MAP));
		if(mapInputKeyCls == null)
			mapInputKeyCls = Text.class;
		if(mapInputValueCls == null)
			mapInputValueCls = Text.class;
		plan.setInputKeyClass(mapInputKeyCls);
		plan.setInputValueClass(mapInputValueCls);
		plan.setOutputKeyClass((Class<? extends Writable>) indexJobConf.getMapOutputKeyClass());
		plan.setOutputValueClass((Class<? extends Writable>) indexJobConf.getMapOutputValueClass());
	}

	private void addReduceToPlan(Plan plan, Reducer reducer) throws JobNotFoundException, InvalidPlanException, TaskNotFoundException {
		plan.addTask(new UserReduceTaskDescriptor(reducer, TaskType.REDUCE));
		
		plan.setInputKeyClass((Class<? extends Writable>) indexJobConf.getMapOutputKeyClass());
		plan.setInputValueClass((Class<? extends Writable>) indexJobConf.getMapOutputValueClass());
		plan.setOutputKeyClass((Class<? extends Writable>) indexJobConf.getOutputKeyClass());
		plan.setOutputValueClass((Class<? extends Writable>) indexJobConf.getOutputValueClass());
		
		int numReudcers = indexJobConf.getNumReduceTasks();
		plan.setNumOfReduces(numReudcers);
	}

	public ParallelPlan selectPlan(ParallelIndexOperator pIdxOp) {
		ParallelPlan pPlan = new ParallelPlan();

		List<SimpleIndexOperator> simpleIdxOps = pIdxOp.getParallelOperators();
		int numOfSimpleIdxOps = simpleIdxOps.size();

		List<Integer> inputList = new ArrayList<Integer>();
		for (int i = 0; i < numOfSimpleIdxOps; i++) {
			inputList.add(i);
		}
		List<List<Integer>> results = new ArrayList<List<Integer>>();
		Permutation.perm(results, inputList, new ArrayList<Integer>());

		int numOfInputRecord = 1000;

		long minCost = Long.MAX_VALUE;

		List<Integer> minPlanList = null;

		// TODO: current--doesn't consider partition, doesn't consider running
		// node
		for (List<Integer> list : results) {
			for (int i = 0; i < numOfSimpleIdxOps; i++) {
				long cost = calculateCost(pIdxOp, list, false, i);
				if (cost < minCost) {
					minCost = cost;
					minPlanList = list;
				}
			}
		}

		pPlan.setPlanId(1);
		pPlan.addSerialTasks(minPlanList);

		return pPlan;
	}

	public long selectPlan(__IndexOperator idxOp, int[] planCode, int numOfInputRecord) {
		//int numOfInputRecord = 1000;
		long minCost = Long.MAX_VALUE;
		int minPlanId = 1;
		int minPlanNodeIndicator = -1;

		IndexOperatorMetadata idxOpMeta = metastore.getIdxOpMeta(idxOp.getClass().toString());
		int numOfIdxes = idxOpMeta.getNumOfIndexes();
	
		if(!idxOpMeta.checkIfMetaAvailable()){			
			return getInitialPlan(idxOp, idxOpMeta, planCode, numOfInputRecord);
		}

		for (int planId = 1; planId < 6; planId++) {
			if (planId == 1) {
				long cost = calculateCost(idxOp, numOfInputRecord, 1, -1);
				System.out.println("COST of Plan 1 : " +  cost);
				if (cost < minCost) {
					minCost = cost;
					minPlanId = 1;
					minPlanNodeIndicator = -1;
				}
			} else if (planId == 2) {
				if(!this.B_USE_NODE_SEL){
					continue;
				}
				for (int i = 0; i < numOfIdxes; i++) {
					if (checkIfCoPartitioned(idxOp, planId, i)) {
						long cost = calculateCost(idxOp, numOfInputRecord, planId, i);
						if (cost < minCost) {
							minCost = cost;
							minPlanId = planId;
							minPlanNodeIndicator = i;
						}
					}
				}
			} else if (planId == 3) {
				if(!this.B_USE_NODE_SEL){
					continue;
				}
				for (int i = 0; i < numOfIdxes; i++) {
					if (checkIfCoPartitioned(idxOp, planId, i)) {
						long cost = calculateCost(idxOp, numOfInputRecord, planId, i);
						if (cost < minCost) {
							minCost = cost;
							minPlanId = planId;
							minPlanNodeIndicator = i;
						}
					}
				}

			} else if (planId == 4) {
				if(!this.B_USE_RE_PART){
					continue;
				}
				for (int i = 0; i < numOfIdxes; i++) {
					if (checkKeyNumber(idxOp, i)) {
						long cost = calculateCost(idxOp, numOfInputRecord, planId, i);
						System.out.println("COST of Plan 4, index " + i + " : " + cost);
						if (cost < minCost) {
							minCost = cost;
							minPlanId = planId;
							minPlanNodeIndicator = i;
						}
					}
				}
				System.out.println("After evaluate plan 4:");
				System.out.println("\tPlanId: " + minPlanId);
				System.out.println("\tMinCost: " + minCost );
			} else if (planId == 5) {
				if(!this.B_USE_NODE_SEL){
					continue;
				}
				if(!this.B_USE_RE_PART){
					continue;
				}
				for (int i = 0; i < numOfIdxes; i++) {
					if (checkIfPartitioned(idxOp, i) && checkKeyConsistent(idxOp, i)) {
						// if (checkIfCoPartitioned(idxOp, planId, i)) {
						// here node indicator means partitioned by the key of
						// this index
						long cost = calculateCost(idxOp, numOfInputRecord, planId, i);
						System.out.println("COST of Plan 5, index " + i + " : " + cost);
						if (cost < minCost) {
							minCost = cost;
							minPlanId = planId;
							minPlanNodeIndicator = i;
						}
					}
					// }
				}
				System.out.println("After evaluate plan 5:");
				System.out.println("\tPlanId: " + minPlanId);
				System.out.println("\tMinCost: " + minCost );
			} else {

			}
		}
		
		planCode[0] = minPlanId;
		planCode[1] = minPlanNodeIndicator;

		return minCost;
	}



	private boolean checkIfPartitioned(__IndexOperator idxOp, int i) {
		return idxOp.getIndexAccessor(i).isPartitioned();
	}

	private boolean checkKeyNumber(__IndexOperator idxOp, int i) {
		IndexOperatorMetadata idxOpMeta = metastore.getIdxOpMeta(idxOp.getClass().toString());
		if(idxOpMeta.getNumOfIndexKeyPerRecord(i) == 1 /*&& idxOp.getIndexAccessor(i).isPartitioned()*/){
			return true;
		}
		return false;
	}
	
	private boolean checkKeyConsistent(__IndexOperator idxOp, int i) {
		IndexOperatorMetadata idxOpMeta = metastore.getIdxOpMeta(idxOp.getClass().toString());
		if(idxOpMeta.getNumOfIndexKeyPerRecord(i) == 1 /*&& idxOp.getIndexAccessor(i).isPartitioned()*/){
			return true;
		}
		return false;
	}

	public boolean checkIfCoPartitioned(__IndexOperator idxOp, int planId, int indexId) {
		if(idxOp.getCoPartitionedWithIndex().contains(indexId)){
			return true;
		}
		return false;
	}
	
	public double getIndexLookupFactor(IndexOperatorMetadata idxOpMeta, int index, int lookupTrafficSize){
		double avgIndexLookupTime = idxOpMeta.getAvgIndexLookupTime(index);
		double factor = 1.0;
		if((((float)lookupTrafficSize) / LOC_BYTES_PER_SECOND) !=0 )
			factor = avgIndexLookupTime / (((double)lookupTrafficSize) / LOC_BYTES_PER_SECOND);
		if(factor < 1){
			factor =  1;
		}
		return factor;
	}

	public long calculateCost(__IndexOperator idxOp, int numOfInputRecord, int planId, int nodeIndicator) {
		long totalCost = 0;

		IndexOperatorMetadata idxOpMeta = metastore.getIdxOpMeta(idxOp.getClass().toString());

		if (planId == 1) {
			int sir = idxOpMeta.getSizeOfInputRecord();
			int numOfIdxes = idxOpMeta.getNumOfIndexes();

			//totalCost += (numOfInputRecord * sir);
			for (int index = 0; index < numOfIdxes; index++) {
				int nikpr = idxOpMeta.getNumOfIndexKeyPerRecord(index);
				int sik = idxOpMeta.getSizeOfIndexKey(index);
				int siv = idxOpMeta.getSizeOfIndexValue(index);
				double cacheMissRate = 1.0 - idxOpMeta.getCacheHitRate(index);
				System.out.println("Plan 1: sir = " + sir + " nir = " + numOfInputRecord + " nikpr = " + nikpr
						+ " sik = " + sik + " siv " + siv);

				double factor = getIndexLookupFactor(idxOpMeta, index, (sik + siv + INDEX_LOOKUP_COMMON_COST));
				System.out.println("Index lookup factor: " + factor);
				totalCost += numOfInputRecord * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST) * cacheMissRate * factor);
			}
			System.out.println("Min cost after Plan 1: " + totalCost);

		} else if (planId == 2) {

			int sir = idxOpMeta.getSizeOfInputRecord();
			int numOfIdxes = idxOpMeta.getNumOfIndexes();

			totalCost += (numOfInputRecord * sir);

			for (int index = 0; index < numOfIdxes; index++) {
				if (index != nodeIndicator) {
					int nikpr = idxOpMeta.getNumOfIndexKeyPerRecord(index);
					int sik = idxOpMeta.getSizeOfIndexKey(index);
					int siv = idxOpMeta.getSizeOfIndexValue(index);
					double factor = getIndexLookupFactor(idxOpMeta, index, (sik + siv + INDEX_LOOKUP_COMMON_COST));
					System.out.println("Index lookup factor: " + factor);
					double cacheMissRate = 1.0 - idxOpMeta.getCacheHitRate(index);
					totalCost += numOfInputRecord * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST) * cacheMissRate * factor);
				}
			}

			totalCost += (numOfInputRecord * sir);
			
			System.out.println("Min cost after Plan 2: " + totalCost);

		} else if (planId == 3) {

			int sir = idxOpMeta.getSizeOfInputRecord();
			int numOfIdxes = idxOpMeta.getNumOfIndexes();
			double selectivity = idxOpMeta.getSeleOfPrePro();

			//totalCost += (numOfInputRecord * sir);

			int svapp = idxOpMeta.getSizeOfValueAfterPrePro();

			totalCost += (numOfInputRecord * selectivity * svapp * 2);

			for (int index = 0; index < numOfIdxes; index++) {
				if (index != nodeIndicator) {
					int nikpr = idxOpMeta.getNumOfIndexKeyPerRecord(index);
					int sik = idxOpMeta.getSizeOfIndexKey(index);
					int siv = idxOpMeta.getSizeOfIndexValue(index);
					double factor = getIndexLookupFactor(idxOpMeta, index, (sik + siv + INDEX_LOOKUP_COMMON_COST));
					System.out.println("Index lookup factor: " + factor);
					double cacheMissRate = 1.0 - idxOpMeta.getCacheHitRate(index);
					totalCost += numOfInputRecord * selectivity * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST)* cacheMissRate *factor);
				}
			}

			System.out.println("Min cost after Plan 3: " + totalCost);
		} else if (planId == 4) {

			int sir = idxOpMeta.getSizeOfInputRecord();
			int numOfIdxes = idxOpMeta.getNumOfIndexes();
			double selectivity = idxOpMeta.getSeleOfPrePro();

			//totalCost += (numOfInputRecord * sir);

			int svapp = idxOpMeta.getSizeOfValueAfterPrePro();

			totalCost += (numOfInputRecord * selectivity * svapp);
			totalCost += this.FIXED_SHUFF_COST;

			for (int index = 0; index < numOfIdxes; index++) {
				int nikpr = idxOpMeta.getNumOfIndexKeyPerRecord(index);
				int sik = idxOpMeta.getSizeOfIndexKey(index);
				int siv = idxOpMeta.getSizeOfIndexValue(index);
				
				double factor = getIndexLookupFactor(idxOpMeta, index, (sik + siv + INDEX_LOOKUP_COMMON_COST));
				System.out.println("Index lookup factor: " + factor);
				if (index != nodeIndicator) {
					totalCost += numOfInputRecord * selectivity * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST)*factor);
				} else {
					double cacheHitRate = idxOpMeta.getCacheHitRate(index);
					double cacheMissRate = (1.0 - cacheHitRate);
					double cardinality = 0.5;//0.3;
					//int cardinality = 1.0/idxOpMeta.getCacheHitRate(index);//100;//idxOpMeta.getCardinalityOfIndexKey(nodeIndicator);
					totalCost += (numOfInputRecord * selectivity * nikpr * cacheMissRate* cardinality * (sik + siv + INDEX_LOOKUP_COMMON_COST)*factor);
				}
			}
			System.out.println("Min cost after Plan 4: " + totalCost);

		} else if (planId == 5) {

			int sir = idxOpMeta.getSizeOfInputRecord();
			int numOfIdxes = idxOpMeta.getNumOfIndexes();
			double selectivity = idxOpMeta.getSeleOfPrePro();

			//totalCost += (numOfInputRecord * sir);

			int svapp = idxOpMeta.getSizeOfValueAfterPrePro();

			//totalCost += (numOfInputRecord * selectivity * svapp); // for MR
																	// output
			totalCost += this.FIXED_SHUFF_COST;

			totalCost += (numOfInputRecord * selectivity * svapp); // copy to
																	// index
																	// node

			for (int index = 0; index < numOfIdxes; index++) {
				int nikpr = idxOpMeta.getNumOfIndexKeyPerRecord(index);
				int sik = idxOpMeta.getSizeOfIndexKey(index);
				int siv = idxOpMeta.getSizeOfIndexValue(index);
				double factor = getIndexLookupFactor(idxOpMeta, index, (sik + siv + INDEX_LOOKUP_COMMON_COST));
				System.out.println("Index lookup factor: " + factor);
				if (index != nodeIndicator) {
					totalCost += numOfInputRecord * selectivity * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST) * factor);
				} else {

				}
			}
			System.out.println("Min cost after Plan 5: " + totalCost);

		} else {

		}

		return totalCost;
	}

	public long calculateCost(ParallelIndexOperator pIdxOp, List<Integer> list, boolean bPartition, int partitionKey) {
		long totalCost = 0;
		IndexOperatorMetadata pIdxOpMeta = metastore.getIdxOpMeta(pIdxOp.getClass().toString());

		int numOfInputRecord = 1000;
		int sir = pIdxOpMeta.getSizeOfInputRecord();

		totalCost += (numOfInputRecord * sir);

		double preProSelectivity = pIdxOpMeta.getSeleOfPrePro();

		for (int index : list) {

			int nikpr = pIdxOpMeta.getNumOfIndexKeyPerRecord(index);
			int sik = pIdxOpMeta.getSizeOfIndexKey(index);
			int siv = pIdxOpMeta.getSizeOfIndexValue(index);
			double selectivity = 1.0;

			for (int i = 0; i < index - 1; i++) {
				if (selectivity > pIdxOpMeta.getSelectivityOfIdxPost(i)) {
					selectivity = pIdxOpMeta.getSelectivityOfIdxPost(i);
				}
			}

			totalCost += ((numOfInputRecord * preProSelectivity) * selectivity * (nikpr * (sik + siv + INDEX_LOOKUP_COMMON_COST)));

		}

		return totalCost;
	}

	public Plan reEvaluate(Plan plan, int ojId,  boolean bMap, int numOfInputRecord) throws Exception {
		int currNumOfRecords = numOfInputRecord;
		
		boolean bNewPlan = false;
		Plan newPlan =  new Plan();
		newPlan.addJob();
		
		Vector<TaskDescriptor> descs = plan.getJob(ojId);	
		System.out.println(descs.toString());
		
		for(int i=0; i<descs.size(); i++){
			TaskDescriptor desc = descs.get(i);
			if(desc instanceof UserMapTaskDescriptor){
				newPlan.addTask(desc);
			}
			if(desc instanceof UserReduceTaskDescriptor){
				newPlan.addTask(desc);
			}
			if(desc instanceof IndexLookupTaskDescriptor || desc instanceof IndexPostTaskDescriptor){
				//newPlan.addTask(desc);
			}
			__IndexOperator idxOp = getIdxOp(desc);
			
			if(idxOp != null){
				long oldPlanCost = 0;
				long newPlanCost = 0;
				IndexOperatorMetadata idxOpMeta = this.metastore.getIdxOpMeta(idxOp.getClass().toString());
				
				if((i+1) < descs.size() && (i+2) < descs.size() && descs.get(i+1) instanceof IndexLookupTaskDescriptor && descs.get(i+2) instanceof IndexPostTaskDescriptor){
			
				}else{
					return null;
				}
				
				System.out.println("Current lines: " + currNumOfRecords);
				
				oldPlanCost = getInitPlanCost(idxOp, currNumOfRecords);					
				newPlanCost = generatePlan(newPlan, idxOp, currNumOfRecords);
				i += 2;
				
				currNumOfRecords = (int)(((double)currNumOfRecords) * idxOpMeta.getSeleOfPrePro());
				currNumOfRecords = (int)(((double)currNumOfRecords) *idxOpMeta.getPostProductivity());
				
				
				//System.out.println("newPlanCost = " + newPlanCost + " oldPlanCost " + oldPlanCost );
				System.out.println(idxOp.getClass().getName() + " oldPlanCost = " + oldPlanCost);
				System.out.println(idxOp.getClass().getName() + " newPlanCost = " + newPlanCost);
				
				if((newPlanCost + THRESH_CHANGE_PLAN) < oldPlanCost){					
					bNewPlan = true;
				}
			}
		}
	
		if(bNewPlan){
			System.out.println("\n\n\nNew plan after re-evaluated:");
			newPlan.print();
			System.out.println("\n\n\n");
			return newPlan;
		}
		return null;
	}
	
	private long getInitPlanCost(__IndexOperator idxOp, int numOfInputRecord) {
		long cost = calculateCost(idxOp, numOfInputRecord, 1, -1);
		return cost;
	}


	private __IndexOperator getIdxOp(TaskDescriptor desc) throws InstantiationException, IllegalAccessException {

		if (desc instanceof IndexPreTaskDescriptor) {
			IndexPreTaskDescriptor iptd = (IndexPreTaskDescriptor) desc;
			__IndexOperator idxOp = iptd.getTask();
			
			java.util.Iterator itHead = indexJobConf.getHeadIndexOperators().iterator();
			while (itHead.hasNext()) {
				Object obj = itHead.next();
				if (obj instanceof __IndexOperator) {
					if(obj.getClass().getName().compareToIgnoreCase(idxOp.getClass().getName()) == 0){
						return (__IndexOperator)obj;
					}
				}
			}
			
			java.util.Iterator itBody = indexJobConf.getBodyIndexOperators().iterator();
			while (itBody.hasNext()) {
				Object obj = itBody.next();
				if (obj instanceof __IndexOperator) {
					if(obj.getClass().getName().compareToIgnoreCase(idxOp.getClass().getName()) == 0){
						return (__IndexOperator)obj;
					}
				}
			}
			
			java.util.Iterator itTail = indexJobConf.getTailIndexOperators().iterator();
			while (itTail.hasNext()) {
				Object obj = itTail.next();
				if (obj instanceof __IndexOperator) {
					if(obj.getClass().getName().compareToIgnoreCase(idxOp.getClass().getName()) == 0){
						return (__IndexOperator)obj;
					}
				}
			}
			
		} else {
			return null;
		}
		return null;
	}

	private void addToPlan(Plan plan, ParallelIndexOperator pIdxOp, ParallelPlan pPlan) {
		int planId = pPlan.getPlanId();

		if (planId == 1) {

		} else {
			System.err.println("Undefined plan!");
		}
	}
	
	public static long getFileSize(){
		long size = 0;
		
		return size;
	}
}
